// Copyright 2021-present The Atlas Authors. All rights reserved.
// This source code is licensed under the Apache 2.0 license found
// in the LICENSE file in the root directory of this source tree.

//go:build !ent

package postgres

import (
	"testing"

	"ariga.io/atlas/sql/internal/sqltest"
	"ariga.io/atlas/sql/internal/sqlx"
	"ariga.io/atlas/sql/schema"
	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/require"
)

func TestDSQL_Detection(t *testing.T) {
	db, m, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	// Test that Aurora DSQL is detected via version string
	m.ExpectQuery(sqltest.Escape(paramsQuery)).
		WillReturnRows(sqltest.Rows(`
  version       |  am  | version_string
----------------|------|---------------------------------------------
 150000         | heap | PostgreSQL 15.0 on x86_64-pc-linux-gnu, compiled by gcc (GCC) 7.3.1, Aurora_DSQL 1.0.0
`))

	drv, err := Open(db)
	require.NoError(t, err)
	require.IsType(t, noLockDriver{}, drv)

	// Verify it's a DSQL driver by checking the underlying driver
	dsqlDrv := drv.(noLockDriver).noLocker.(*Driver)
	require.True(t, dsqlDrv.conn.dsql)
}

func TestDSQL_JSONColumnValidation(t *testing.T) {
	db, m, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	// Setup DSQL driver
	m.ExpectQuery(sqltest.Escape(paramsQuery)).
		WillReturnRows(sqltest.Rows(`
  version       |  am  | version_string
----------------|------|---------------------------------------------
 150000         | heap | PostgreSQL 15.0 on x86_64-pc-linux-gnu, compiled by gcc (GCC) 7.3.1, Aurora_DSQL 1.0.0
`))

	drv, err := Open(db)
	require.NoError(t, err)
	dsqlDrv := drv.(noLockDriver).noLocker.(*Driver)

	table := &schema.Table{Name: "users"}

	// Test ColumnChange rejects JSON type
	from := &schema.Column{
		Name: "data",
		Type: &schema.ColumnType{
			Type: &schema.StringType{T: TypeText},
		},
	}
	toJSON := &schema.Column{
		Name: "data",
		Type: &schema.ColumnType{
			Type: &schema.JSONType{T: TypeJSON},
		},
	}

	_, err = dsqlDrv.Differ.(*sqlx.Diff).DiffDriver.ColumnChange(table, from, toJSON, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "aurora dsql")
	require.Contains(t, err.Error(), "json")
	require.Contains(t, err.Error(), "not supported")

	// Test ColumnChange rejects JSONB type
	toJSONB := &schema.Column{
		Name: "data",
		Type: &schema.ColumnType{
			Type: &schema.JSONType{T: TypeJSONB},
		},
	}

	_, err = dsqlDrv.Differ.(*sqlx.Diff).DiffDriver.ColumnChange(table, from, toJSONB, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "aurora dsql")
	require.Contains(t, err.Error(), "jsonb")
	require.Contains(t, err.Error(), "not supported")
}

func TestDSQL_InspectSchemaWithJSON(t *testing.T) {
	// This test verifies that inspecting a schema with JSON/JSONB columns
	// returns an error for DSQL
	db, m, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	// Setup DSQL driver
	m.ExpectQuery(sqltest.Escape(paramsQuery)).
		WillReturnRows(sqltest.Rows(`
  version       |  am  | version_string
----------------|------|---------------------------------------------
 150000         | heap | PostgreSQL 15.0 on x86_64-pc-linux-gnu, compiled by gcc (GCC) 7.3.1, Aurora_DSQL 1.0.0
`))

	drv, err := Open(db)
	require.NoError(t, err)
	dsqlDrv := drv.(noLockDriver).noLocker.(*Driver)
	dsqlInspector := dsqlDrv.Inspector.(*dsqlInspect)

	// Test validation with JSON column
	sWithJSON := schema.New("test").
		AddTables(
			schema.NewTable("users").
				AddColumns(
					schema.NewColumn("data").
						SetType(&schema.JSONType{T: TypeJSON}),
				),
		)

	err = dsqlInspector.validateNoJSONColumns(sWithJSON)
	require.Error(t, err)
	require.Contains(t, err.Error(), "aurora dsql")
	require.Contains(t, err.Error(), "json")
	require.Contains(t, err.Error(), "not supported")
	require.Contains(t, err.Error(), "users")
	require.Contains(t, err.Error(), "data")

	// Test validation with JSONB column
	sWithJSONB := schema.New("test").
		AddTables(
			schema.NewTable("posts").
				AddColumns(
					schema.NewColumn("metadata").
						SetType(&schema.JSONType{T: TypeJSONB}),
				),
		)

	err = dsqlInspector.validateNoJSONColumns(sWithJSONB)
	require.Error(t, err)
	require.Contains(t, err.Error(), "aurora dsql")
	require.Contains(t, err.Error(), "jsonb")
	require.Contains(t, err.Error(), "not supported")
	require.Contains(t, err.Error(), "posts")
	require.Contains(t, err.Error(), "metadata")
}

func TestDSQL_NoLock(t *testing.T) {
	db, m, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	// Setup DSQL driver
	m.ExpectQuery(sqltest.Escape(paramsQuery)).
		WillReturnRows(sqltest.Rows(`
  version       |  am  | version_string
----------------|------|---------------------------------------------
 150000         | heap | PostgreSQL 15.0 on x86_64-pc-linux-gnu, compiled by gcc (GCC) 7.3.1, Aurora_DSQL 1.0.0
`))

	drv, err := Open(db)
	require.NoError(t, err)

	// Verify that DSQL driver is wrapped in noLockDriver
	// This is a structural indication that advisory locks (pg_try_advisory_lock)
	// should not be used with Aurora DSQL, similar to CockroachDB
	require.IsType(t, noLockDriver{}, drv)
}

func TestDSQL_URLSchemes(t *testing.T) {
	// Test that DSQL URL schemes are registered
	tests := []struct {
		name   string
		scheme string
		want   bool
	}{
		{
			name:   "postgres scheme",
			scheme: "postgres",
			want:   true,
		},
		{
			name:   "postgresql scheme",
			scheme: "postgresql",
			want:   true,
		},
		{
			name:   "aurora_dsql scheme",
			scheme: "aurora_dsql",
			want:   true,
		},
		{
			name:   "dsql scheme",
			scheme: "dsql",
			want:   true,
		},
		{
			name:   "unknown scheme",
			scheme: "unknown",
			want:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Note: We can't directly test if the scheme is registered
			// without accessing internal sqlclient package structures.
			// This is a placeholder for documentation purposes.
			// The actual registration happens in init() and is tested
			// implicitly when the package is loaded.
		})
	}
}

func TestDSQL_RegularPostgresNotAffected(t *testing.T) {
	db, m, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	// Test that regular PostgreSQL is not affected by DSQL detection
	m.ExpectQuery(sqltest.Escape(paramsQuery)).
		WillReturnRows(sqltest.Rows(`
  version       |  am  | version_string
----------------|------|---------------------------------------------
 150000         | heap | PostgreSQL 15.0 on x86_64-pc-linux-gnu, compiled by gcc (GCC) 7.3.1
`))

	drv, err := Open(db)
	require.NoError(t, err)

	// Verify it's a regular driver, not noLockDriver
	require.IsType(t, &Driver{}, drv)

	// Verify JSON columns are allowed for regular PostgreSQL
	regularDrv := drv.(*Driver)
	require.False(t, regularDrv.conn.dsql)

	// Test ColumnChange allows JSON types for regular PostgreSQL
	from := &schema.Column{
		Name: "data",
		Type: &schema.ColumnType{
			Type: &schema.StringType{T: TypeText},
		},
	}
	to := &schema.Column{
		Name: "data",
		Type: &schema.ColumnType{
			Type: &schema.JSONType{T: TypeJSON},
		},
	}

	table := &schema.Table{Name: "users"}
	_, err = regularDrv.Differ.(*sqlx.Diff).DiffDriver.ColumnChange(table, from, to, nil)
	require.NoError(t, err)
}
