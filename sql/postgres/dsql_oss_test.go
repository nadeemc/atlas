// Copyright 2021-present The Atlas Authors. All rights reserved.
// This source code is licensed under the Apache 2.0 license found
// in the LICENSE file in the root directory of this source tree.

//go:build !ent

package postgres

import (
	"net/url"
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

func TestDSQL_JSONColumnConversion(t *testing.T) {
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

	// Test ColumnChange converts JSON type to text
	// When changing from integer to JSON, it should be converted to integer to text
	from := &schema.Column{
		Name: "data",
		Type: &schema.ColumnType{
			Type: &schema.IntegerType{T: TypeInteger},
		},
	}
	toJSON := &schema.Column{
		Name: "data",
		Type: &schema.ColumnType{
			Type: &schema.JSONType{T: TypeJSON},
		},
	}

	change, err := dsqlDrv.Differ.(*sqlx.Diff).DiffDriver.ColumnChange(table, from, toJSON, nil)
	require.NoError(t, err)
	// Change should be detected (integer -> text)
	require.NotEqual(t, sqlx.NoChange, change)

	// Test ColumnChange converts JSONB type to text
	toJSONB := &schema.Column{
		Name: "data",
		Type: &schema.ColumnType{
			Type: &schema.JSONType{T: TypeJSONB},
		},
	}

	change, err = dsqlDrv.Differ.(*sqlx.Diff).DiffDriver.ColumnChange(table, from, toJSONB, nil)
	require.NoError(t, err)
	// Change should be detected (integer -> text)
	require.NotEqual(t, sqlx.NoChange, change)
	
	// Test that changing from text to JSON results in no change (both are text in DSQL)
	fromText := &schema.Column{
		Name: "data",
		Type: &schema.ColumnType{
			Type: &schema.StringType{T: TypeText},
		},
	}
	
	change, err = dsqlDrv.Differ.(*sqlx.Diff).DiffDriver.ColumnChange(table, fromText, toJSON, nil)
	require.NoError(t, err)
	// No change should be detected (text -> text)
	require.Equal(t, sqlx.NoChange, change)
}

func TestDSQL_InspectSchemaWithJSON(t *testing.T) {
	// This test verifies that inspecting a schema with JSON/JSONB columns
	// converts them to text for DSQL
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

	// Test conversion with JSON column
	sWithJSON := schema.New("test").
		AddTables(
			schema.NewTable("users").
				AddColumns(
					schema.NewColumn("data").
						SetType(&schema.JSONType{T: TypeJSON}),
				),
		)

	dsqlInspector.convertJSONToText(sWithJSON)
	
	// Verify the column was converted to text
	table, ok := sWithJSON.Table("users")
	require.True(t, ok)
	col, ok := table.Column("data")
	require.True(t, ok)
	stringType, ok := col.Type.Type.(*schema.StringType)
	require.True(t, ok)
	require.Equal(t, TypeText, stringType.T)

	// Test conversion with JSONB column
	sWithJSONB := schema.New("test").
		AddTables(
			schema.NewTable("posts").
				AddColumns(
					schema.NewColumn("metadata").
						SetType(&schema.JSONType{T: TypeJSONB}),
				),
		)

	dsqlInspector.convertJSONToText(sWithJSONB)
	
	// Verify the column was converted to text
	table, ok = sWithJSONB.Table("posts")
	require.True(t, ok)
	col, ok = table.Column("metadata")
	require.True(t, ok)
	stringType, ok = col.Type.Type.(*schema.StringType)
	require.True(t, ok)
	require.Equal(t, TypeText, stringType.T)
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
	// Test that DSQL URL schemes are converted to postgres:// for DSN
	tests := []struct {
		name        string
		url         string
		expectedDSN string
	}{
		{
			name:        "postgres scheme unchanged",
			url:         "postgres://user:pass@host:5432/dbname?sslmode=disable",
			expectedDSN: "postgres://user:pass@host:5432/dbname?sslmode=disable",
		},
		{
			name:        "postgresql scheme unchanged",
			url:         "postgresql://user:pass@host:5432/dbname?sslmode=disable",
			expectedDSN: "postgresql://user:pass@host:5432/dbname?sslmode=disable",
		},
		{
			name:        "dsql scheme converted to postgres",
			url:         "dsql://user:pass@host:5432/dbname?sslmode=disable",
			expectedDSN: "postgres://user:pass@host:5432/dbname?sslmode=disable",
		},
	}

	p := parser{}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			u, err := url.Parse(tt.url)
			require.NoError(t, err)
			
			result := p.ParseURL(u)
			require.Equal(t, tt.expectedDSN, result.DSN)
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
