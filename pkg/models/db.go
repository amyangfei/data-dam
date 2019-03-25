package models

import (
	"context"
)

// DBCreator creates a database layer
type DBCreator interface {
	Create(cfg DBConfig) (DB, error)
}

// DB is the layer to access the database
type DB interface {
	// Close closes the database layer.
	Close() error

	// Update updates a record in the database.
	Update(ctx context.Context, schema, table string, keys map[string]interface{}, values map[string]interface{}) error

	// Insert inserts a record in the database.
	Insert(ctx context.Context, schema, table string, values map[string]interface{}) error

	// Delete deletes a record from the database.
	Delete(ctx context.Context, schema, table string, keys map[string]interface{}) error
}
