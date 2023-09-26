// Copyright 2021-present The Atlas Authors. All rights reserved.
// This source code is licensed under the Apache 2.0 license found
// in the LICENSE file in the root directory of this source tree.

package schema

type (
	// A Realm or a database describes a domain of schema resources that are logically connected
	// and can be accessed and queried in the same connection (e.g. a physical database instance).
	Realm struct {
		Schemas []*Schema
		Attrs   []Attr
	}

	// A Schema describes a database schema (i.e. named database).
	Schema struct {
		Name    string
		Realm   *Realm
		Tables  []*Table
		Views   []*View
		Attrs   []Attr   // Attrs and options.
		Objects []Object // Driver specific objects.
	}

	// An Object represents a generic database object.
	// Note that this interface is implemented by some top-level types
	// to describe their relationship, and by driver specific types.
	Object interface {
		obj()
	}

	// A Table represents a table definition.
	Table struct {
		Name        string
		Schema      *Schema
		Columns     []*Column
		Indexes     []*Index
		PrimaryKey  *Index
		ForeignKeys []*ForeignKey
		Attrs       []Attr // Attrs, constraints and options.
	}

	// A View represents a view definition.
	View struct {
		Name    string
		Def     string
		Schema  *Schema
		Columns []*Column
		Attrs   []Attr   // Attrs and options.
		Deps    []Object // Tables and views used in view definition.
		Indexes []*Index // Indexes on materialized view.
	}

	// A Column represents a column definition.
	Column struct {
		Name    string
		Type    *ColumnType
		Default Expr
		Attrs   []Attr
		Indexes []*Index
		// Foreign keys that this column is
		// part of their child columns.
		ForeignKeys []*ForeignKey
	}

	// ColumnType represents a column type that is implemented by the dialect.
	ColumnType struct {
		Type Type
		Raw  string
		Null bool
	}

	// An Index represents an index definition.
	Index struct {
		Name   string
		Unique bool
		// Table or View that this index belongs to.
		Table *Table
		View  *View
		Attrs []Attr
		Parts []*IndexPart
	}

	// An IndexPart represents an index part that
	// can be either an expression or a column.
	IndexPart struct {
		// SeqNo represents the sequence number of the key part
		// in the index.
		SeqNo int
		// Desc indicates if the key part is stored in descending
		// order. All databases use ascending order as default.
		Desc  bool
		X     Expr
		C     *Column
		Attrs []Attr
	}

	// A ForeignKey represents an index definition.
	ForeignKey struct {
		Symbol     string
		Table      *Table
		Columns    []*Column
		RefTable   *Table
		RefColumns []*Column
		OnUpdate   ReferenceOption
		OnDelete   ReferenceOption
	}
)

// Schema returns the first schema that matched the given name.
func (r *Realm) Schema(name string) (*Schema, bool) {
	for _, s := range r.Schemas {
		if s.Name == name {
			return s, true
		}
	}
	return nil, false
}

// Table returns the first table that matched the given name.
func (s *Schema) Table(name string) (*Table, bool) {
	for _, t := range s.Tables {
		if t.Name == name {
			return t, true
		}
	}
	return nil, false
}

// View returns the first view that matched the given name.
func (s *Schema) View(name string) (*View, bool) {
	for _, v := range s.Views {
		if v.Name == name && !v.Materialized() {
			return v, true
		}
	}
	return nil, false
}

// Materialized returns the first materialized view that matched the given name.
func (s *Schema) Materialized(name string) (*View, bool) {
	for _, v := range s.Views {
		if v.Name == name && v.Materialized() {
			return v, true
		}
	}
	return nil, false
}

// Object returns the first object that matched the given predicate.
func (s *Schema) Object(f func(Object) bool) (Object, bool) {
	for _, o := range s.Objects {
		if f(o) {
			return o, true
		}
	}
	return nil, false
}

// Column returns the first column that matched the given name.
func (t *Table) Column(name string) (*Column, bool) {
	for _, c := range t.Columns {
		if c.Name == name {
			return c, true
		}
	}
	return nil, false
}

// Index returns the first index that matched the given name.
func (t *Table) Index(name string) (*Index, bool) {
	for _, i := range t.Indexes {
		if i.Name == name {
			return i, true
		}
	}
	return nil, false
}

// ForeignKey returns the first foreign-key that matched the given symbol (constraint name).
func (t *Table) ForeignKey(symbol string) (*ForeignKey, bool) {
	for _, f := range t.ForeignKeys {
		if f.Symbol == symbol {
			return f, true
		}
	}
	return nil, false
}

// Materialized reports if the view is materialized.
func (v *View) Materialized() bool {
	for _, a := range v.Attrs {
		if _, ok := a.(*Materialized); ok {
			return true
		}
	}
	return false
}

// SetMaterialized reports if the view is materialized.
func (v *View) SetMaterialized(b bool) *View {
	if b {
		ReplaceOrAppend(&v.Attrs, &Materialized{})
	} else {
		v.Attrs = RemoveAttr[*Materialized](v.Attrs)
	}
	return v
}

// Column returns the first column that matched the given name.
func (v *View) Column(name string) (*Column, bool) {
	for _, c := range v.Columns {
		if c.Name == name {
			return c, true
		}
	}
	return nil, false
}

// Index returns the first index that matched the given name.
func (v *View) Index(name string) (*Index, bool) {
	for _, i := range v.Indexes {
		if i.Name == name {
			return i, true
		}
	}
	return nil, false
}

// AsTable returns a table that represents the view.
func (v *View) AsTable() *Table {
	return NewTable(v.Name).
		SetSchema(v.Schema).
		AddColumns(v.Columns...)
}

// Column returns the first column that matches the given name.
func (f *ForeignKey) Column(name string) (*Column, bool) {
	for _, c := range f.Columns {
		if c.Name == name {
			return c, true
		}
	}
	return nil, false
}

// RefColumn returns the first referenced column that matches the given name.
func (f *ForeignKey) RefColumn(name string) (*Column, bool) {
	for _, c := range f.RefColumns {
		if c.Name == name {
			return c, true
		}
	}
	return nil, false
}

// ReferenceOption for constraint actions.
type ReferenceOption string

// Reference options (actions) specified by ON UPDATE and ON DELETE
// subclauses of the FOREIGN KEY clause.
const (
	NoAction   ReferenceOption = "NO ACTION"
	Restrict   ReferenceOption = "RESTRICT"
	Cascade    ReferenceOption = "CASCADE"
	SetNull    ReferenceOption = "SET NULL"
	SetDefault ReferenceOption = "SET DEFAULT"
)

type (
	// A Type represents a database type. The types below implements this
	// interface and can be used for describing schemas.
	//
	// The Type interface can also be implemented outside this package as follows:
	//
	//	type SpatialType struct {
	//		schema.Type
	//		T string
	//	}
	//
	//	var t schema.Type = &SpatialType{T: "point"}
	//
	Type interface {
		typ()
	}

	// EnumType represents an enum type.
	EnumType struct {
		T      string   // Optional type.
		Values []string // Enum values.
		Schema *Schema  // Optional schema.
	}

	// BinaryType represents a type that stores a binary data.
	BinaryType struct {
		T    string
		Size *int
	}

	// StringType represents a string type.
	StringType struct {
		T    string
		Size int
	}

	// BoolType represents a boolean type.
	BoolType struct {
		T string
	}

	// IntegerType represents an int type.
	IntegerType struct {
		T        string
		Unsigned bool
		Attrs    []Attr
	}

	// DecimalType represents a fixed-point type that stores exact numeric values.
	DecimalType struct {
		T         string
		Precision int
		Scale     int
		Unsigned  bool
	}

	// FloatType represents a floating-point type that stores approximate numeric values.
	FloatType struct {
		T         string
		Unsigned  bool
		Precision int
	}

	// TimeType represents a date/time type.
	TimeType struct {
		T         string
		Precision *int
		Scale     *int
	}

	// JSONType represents a JSON type.
	JSONType struct {
		T string
	}

	// SpatialType represents a spatial/geometric type.
	SpatialType struct {
		T string
	}

	// A UUIDType defines a UUID type.
	UUIDType struct {
		T string
	}

	// UnsupportedType represents a type that is not supported by the drivers.
	UnsupportedType struct {
		T string
	}
)

type (
	// Expr defines an SQL expression in schema DDL.
	//
	// The Expr interface can also be implemented outside this package as follows:
	//
	// 	type NamedDefault struct {
	// 		schema.Expr
	// 		Name string
	// 	}
	// 	// Underlying returns the underlying expression.
	// 	func (e *NamedDefault) Underlying() schema.Expr { return e.Expr }
	//
	//  var e schema.Expr = &NamedDefault{Expr: &schema.Literal{V: "bar"}, Name: "foo"}
	Expr interface {
		expr()
	}

	// Literal represents a basic literal expression like 1, or '1'.
	// String literals are usually quoted with single or double quotes.
	Literal struct {
		V string
	}

	// RawExpr represents a raw expression like "uuid()" or "current_timestamp()".
	// Unlike literals, raw expression are usually inlined as is on migration.
	RawExpr struct {
		X string
	}
)

type (
	// Attr represents the interface that all attributes implement.
	Attr interface {
		attr()
	}

	// Comment describes a schema element comment.
	Comment struct {
		Text string
	}

	// Charset describes a column or a table character-set setting.
	Charset struct {
		V string
	}

	// Collation describes a column or a table collation setting.
	Collation struct {
		V string
	}

	// Check describes a CHECK constraint.
	Check struct {
		Name  string // Optional constraint name.
		Expr  string // Actual CHECK.
		Attrs []Attr // Additional attributes (e.g. ENFORCED).
	}

	// GeneratedExpr describes the expression used for generating
	// the value of a generated/virtual column.
	GeneratedExpr struct {
		Expr string
		Type string // Optional type. e.g. STORED or VIRTUAL.
	}

	// ViewCheckOption describes the standard 'WITH CHECK OPTION clause' of a view.
	ViewCheckOption struct {
		V string // LOCAL, CASCADED, NONE, or driver specific.
	}

	// Materialized is a schema attribute that attached to views to indicates
	// they are MATERIALIZED VIEWs.
	Materialized struct {
		Attr
	}
)

// A list of known view check options.
const (
	ViewCheckOptionNone     = "NONE"
	ViewCheckOptionLocal    = "LOCAL"
	ViewCheckOptionCascaded = "CASCADED"
)

// objects.
func (*Table) obj()    {}
func (*View) obj()     {}
func (*EnumType) obj() {}

// expressions.
func (*Literal) expr() {}
func (*RawExpr) expr() {}

// types.
func (*BoolType) typ()        {}
func (*EnumType) typ()        {}
func (*TimeType) typ()        {}
func (*JSONType) typ()        {}
func (*FloatType) typ()       {}
func (*StringType) typ()      {}
func (*BinaryType) typ()      {}
func (*SpatialType) typ()     {}
func (*UUIDType) typ()        {}
func (*IntegerType) typ()     {}
func (*DecimalType) typ()     {}
func (*UnsupportedType) typ() {}

// attributes.
func (*Check) attr()           {}
func (*Comment) attr()         {}
func (*Charset) attr()         {}
func (*Collation) attr()       {}
func (*GeneratedExpr) attr()   {}
func (*ViewCheckOption) attr() {}

// UnderlyingExpr returns the underlying expression of x.
func UnderlyingExpr(x Expr) Expr {
	if w, ok := x.(interface{ Underlying() Expr }); ok {
		return UnderlyingExpr(w.Underlying())
	}
	return x
}
