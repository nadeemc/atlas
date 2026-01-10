// Copyright 2021-present The Atlas Authors. All rights reserved.
// This source code is licensed under the Apache 2.0 license found
// in the LICENSE file in the root directory of this source tree.

//go:build !ent

package postgres

import (
	"context"
	"fmt"
	"time"

	"ariga.io/atlas/sql/migrate"
	"ariga.io/atlas/sql/schema"
)

type (
	// dsqlDiff implements diff logic for Aurora DSQL, which is PostgreSQL-compatible
	// but has certain limitations. Aurora DSQL does not support:
	// - JSON/JSONB column types
	// - Advisory locks (pg_try_advisory_lock, pg_advisory_lock)
	dsqlDiff struct{ diff }

	// dsqlInspect implements inspection logic for Aurora DSQL, validating that
	// schemas don't contain unsupported features like JSON column types.
	dsqlInspect struct{ inspect }

	// noLocker is an interface that represents a driver that does not support
	// advisory locks. This is used for Aurora DSQL which doesn't support
	// pg_try_advisory_lock or pg_advisory_lock.
	noLocker interface {
		migrate.Driver
		schema.Normalizer
	}

	// noLockDriver wraps a noLocker to indicate that the driver doesn't
	// support advisory lock functionality.
	noLockDriver struct {
		noLocker
	}
)

// Lock implements the schema.Locker interface for noLockDriver.
// Aurora DSQL does not support advisory locks (pg_try_advisory_lock, pg_advisory_lock),
// so this implementation uses a table-based locking mechanism as a workaround.
// It creates a lock table and uses row-level locking to ensure mutual exclusion.
func (d noLockDriver) Lock(ctx context.Context, name string, timeout time.Duration) (schema.UnlockFunc, error) {
	conn, ok := d.noLocker.(*Driver)
	if !ok {
		return nil, fmt.Errorf("postgres: unexpected driver type for DSQL locking")
	}
	return tableLock(ctx, conn.ExecQuerier, name, timeout)
}

// InspectSchema inspects and returns the schema description for Aurora DSQL.
// Converts JSON/JSONB columns to text, as Aurora DSQL doesn't support JSON column types
// but treats them as text internally.
func (i *dsqlInspect) InspectSchema(ctx context.Context, name string, opts *schema.InspectOptions) (*schema.Schema, error) {
	s, err := i.inspect.InspectSchema(ctx, name, opts)
	if err != nil {
		return nil, err
	}
	i.convertJSONToText(s)
	return s, nil
}

// InspectRealm inspects and returns the realm description for Aurora DSQL.
// Converts JSON/JSONB columns to text, as Aurora DSQL doesn't support JSON column types
// but treats them as text internally.
func (i *dsqlInspect) InspectRealm(ctx context.Context, opts *schema.InspectRealmOption) (*schema.Realm, error) {
	r, err := i.inspect.InspectRealm(ctx, opts)
	if err != nil {
		return nil, err
	}
	for _, s := range r.Schemas {
		i.convertJSONToText(s)
	}
	return r, nil
}

// convertJSONToText converts JSON and JSONB column types to text.
// Aurora DSQL does not support JSON column types, as documented in the
// Aurora DSQL limitations: https://docs.aws.amazon.com/aurora-dsql/latest/userguide/working-with-postgresql-compatibility-unsupported-features.html
// However, it supports returning JSON in SELECT statements by casting text to json.
// This method converts JSON/JSONB types to text for column definitions.
func (i *dsqlInspect) convertJSONToText(s *schema.Schema) {
	for _, t := range s.Tables {
		for _, c := range t.Columns {
			if _, ok := c.Type.Type.(*schema.JSONType); ok {
				// Convert JSON/JSONB to text type
				c.Type.Type = &schema.StringType{T: TypeText}
				c.Type.Raw = TypeText
			}
		}
	}
}

// ColumnChange handles column changes for Aurora DSQL.
// Converts JSON/JSONB types to text, as Aurora DSQL doesn't support JSON column types
// but can return JSON data via casting in SELECT statements.
func (dd *dsqlDiff) ColumnChange(fromT *schema.Table, from, to *schema.Column, opts *schema.DiffOptions) (schema.Change, error) {
	// Convert JSON/JSONB to text for Aurora DSQL
	if _, ok := to.Type.Type.(*schema.JSONType); ok {
		// Create a modified column with text type instead of JSON, preserving all other attributes
		modifiedTo := *to
		modifiedTo.Type = &schema.ColumnType{
			Type: &schema.StringType{T: TypeText},
			Raw:  TypeText,
			Null: to.Type.Null,
		}
		return dd.diff.ColumnChange(fromT, from, &modifiedTo, opts)
	}
	return dd.diff.ColumnChange(fromT, from, to, opts)
}

// tableLock implements table-based locking for Aurora DSQL.
// It creates a dedicated lock table and uses row-level locking to ensure mutual exclusion.
// This is used as a workaround for the lack of advisory lock support in Aurora DSQL.
func tableLock(ctx context.Context, conn schema.ExecQuerier, name string, timeout time.Duration) (schema.UnlockFunc, error) {
	// Create lock table if it doesn't exist
	createTableSQL := `
		CREATE TABLE IF NOT EXISTS atlas_migration_locks (
			lock_id TEXT PRIMARY KEY,
			locked_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
			locked_by TEXT
		)
	`
	if _, err := conn.ExecContext(ctx, createTableSQL); err != nil {
		return nil, fmt.Errorf("postgres: creating lock table: %w", err)
	}

	// Try to acquire the lock by inserting a row.
	// INSERT ... ON CONFLICT provides atomicity without explicit transactions.
	var (
		start    = time.Now()
		lockInfo = fmt.Sprintf("atlas-lock-%d", time.Now().UnixNano())
	)

	for {
		// Try to insert the lock row
		insertSQL := `
			INSERT INTO atlas_migration_locks (lock_id, locked_by)
			VALUES ($1, $2)
			ON CONFLICT (lock_id) DO NOTHING
		`
		result, err := conn.ExecContext(ctx, insertSQL, name, lockInfo)
		if err != nil {
			return nil, fmt.Errorf("postgres: acquiring lock: %w", err)
		}

		// Check if we successfully inserted the row
		rowsAffected, err := result.RowsAffected()
		if err != nil {
			return nil, fmt.Errorf("postgres: checking lock acquisition: %w", err)
		}

		if rowsAffected > 0 {
			// Successfully acquired the lock
			return func() error {
				// Release the lock by deleting the row.
				// Use a background context with timeout to ensure unlock completes
				// even if the original context was cancelled.
				unlockCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				
				deleteSQL := `DELETE FROM atlas_migration_locks WHERE lock_id = $1 AND locked_by = $2`
				_, err := conn.ExecContext(unlockCtx, deleteSQL, name, lockInfo)
				if err != nil {
					return fmt.Errorf("postgres: releasing lock: %w", err)
				}
				return nil
			}, nil
		}

		// Lock is held by another process, check timeout
		if time.Since(start) > timeout {
			return nil, schema.ErrLocked
		}

		// Wait before retrying
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(100 * time.Millisecond):
			// Continue to next iteration
		}
	}
}
