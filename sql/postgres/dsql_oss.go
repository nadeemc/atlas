// Copyright 2021-present The Atlas Authors. All rights reserved.
// This source code is licensed under the Apache 2.0 license found
// in the LICENSE file in the root directory of this source tree.

//go:build !ent

package postgres

import (
	"context"
	"fmt"

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
)

// InspectSchema inspects and returns the schema description for Aurora DSQL.
// Returns an error if the schema contains JSON/JSONB columns, which are not supported by DSQL.
func (i *dsqlInspect) InspectSchema(ctx context.Context, name string, opts *schema.InspectOptions) (*schema.Schema, error) {
	s, err := i.inspect.InspectSchema(ctx, name, opts)
	if err != nil {
		return nil, err
	}
	if err := i.validateNoJSONColumns(s); err != nil {
		return nil, err
	}
	return s, nil
}

// InspectRealm inspects and returns the realm description for Aurora DSQL.
// Returns an error if any schema contains JSON/JSONB columns, which are not supported by DSQL.
func (i *dsqlInspect) InspectRealm(ctx context.Context, opts *schema.InspectRealmOption) (*schema.Realm, error) {
	r, err := i.inspect.InspectRealm(ctx, opts)
	if err != nil {
		return nil, err
	}
	for _, s := range r.Schemas {
		if err := i.validateNoJSONColumns(s); err != nil {
			return nil, err
		}
	}
	return r, nil
}

// validateNoJSONColumns checks that no JSON or JSONB columns are present.
// Aurora DSQL does not support JSON column types, as documented in the
// Aurora DSQL limitations: https://docs.aws.amazon.com/aurora-dsql/latest/userguide/working-with-postgresql-compatibility-unsupported-features.html
// Both TypeJSON and TypeJSONB are represented by schema.JSONType with different T values.
func (i *dsqlInspect) validateNoJSONColumns(s *schema.Schema) error {
	for _, t := range s.Tables {
		for _, c := range t.Columns {
			if jsonType, ok := c.Type.Type.(*schema.JSONType); ok {
				return fmt.Errorf("aurora dsql: JSON column type %q is not supported in table %q, column %q", jsonType.T, t.Name, c.Name)
			}
		}
	}
	return nil
}

// ColumnChange handles column changes for Aurora DSQL.
// Returns an error if attempting to create or modify a column to use JSON/JSONB type,
// which is not supported by Aurora DSQL.
// Both TypeJSON and TypeJSONB are represented by schema.JSONType with different T values.
func (dd *dsqlDiff) ColumnChange(fromT *schema.Table, from, to *schema.Column, opts *schema.DiffOptions) (schema.Change, error) {
	// Validate that we're not trying to create or modify a column to use JSON type
	if jsonType, ok := to.Type.Type.(*schema.JSONType); ok {
		return nil, fmt.Errorf("aurora dsql: JSON column type %q is not supported", jsonType.T)
	}
	return dd.diff.ColumnChange(fromT, from, to, opts)
}
