package models

import (
	"context"
	"fmt"
)

// Column stores column information
type Column struct {
	Idx      int
	Name     string
	NotNull  bool
	Unsigned bool
	Tp       string
	Extra    string
}

// Table stores table information
type Table struct {
	Schema string
	Name   string

	Columns      []*Column
	IndexColumns map[string][]*Column
}

// DMLParams stores a DML information
type DMLParams struct {
	Type   OpType
	Schema string
	Table  string
	Keys   map[string]interface{}
	Values map[string]interface{}
}

// DBCreator creates a database layer
type DBCreator interface {
	Create(cfg *DBConfig) (DB, error)
}

// DB is the layer to access the database
type DB interface {
	// Close closes the database layer.
	Close() error

	// PrepareTables scan tables from schema
	// returns `table info slice`, `a slice of column names slice` and whether error happens
	PrepareTables(ctx context.Context, schema string) ([]*Table, [][]string, error)

	// GetTable gets table information from database.
	// returns: `talbe info`, `column names slice` and whether error happens
	GetTable(ctx context.Context, schema, table string) (*Table, []string, error)

	// Update updates a record in the database.
	Update(ctx context.Context, schema, table string, keys map[string]interface{}, values map[string]interface{}) error

	// Insert inserts a record in the database.
	Insert(ctx context.Context, schema, table string, values map[string]interface{}) error

	// Delete deletes a record from the database.
	Delete(ctx context.Context, schema, table string, keys map[string]interface{}) error

	// GenerateDML generates a DML record.
	GenerateDML(ctx context.Context, opType OpType) (*DMLParams, error)
}

var dbCreators = map[string]DBCreator{}

// RegisterDBCreator registers a creator for the database
func RegisterDBCreator(name string, creator DBCreator) {
	_, ok := dbCreators[name]
	if ok {
		panic(fmt.Sprintf("duplicate register database %s", name))
	}
	dbCreators[name] = creator
}

// GetDBCreator gets the DBCreator for the database
func GetDBCreator(name string) DBCreator {
	return dbCreators[name]
}
