// Copyright 2021-present The Atlas Authors. All rights reserved.
// This source code is licensed under the Apache 2.0 license found
// in the LICENSE file in the root directory of this source tree.

//go:build !ent

package postgres

import (
	"context"

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
