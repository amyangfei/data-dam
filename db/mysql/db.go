package mysql

import (
	"database/sql"
	"fmt"

	"github.com/pingcap/errors"

	"github.com/amyangfei/data-dam/pkg/log"
	"github.com/amyangfei/data-dam/pkg/models"
)

const (
	defaultTimeout = 3
)

// MySQLDB implements models.DB
type MySQLDB struct {
	db      *sql.DB
	verbose bool
}

// Create implements Create of models.DB
func (d *MySQLDB) Create(cfg models.DBConfig) (models.DB, error) {
	d := &MySQLDB{
		verbose: cfg.Verbose,
	}
	db, err := createDB(cfg.MySQL)
	if err != nil {
		if db != nil {
			db.Close()
		}
		return nil, errors.Trace(err)
	}
	d.db = db
	return d, nil
}

func createDB(cfg models.MySQLConfig) (*sql.DB, error) {
	dsn := fmt.Sprintf(
		"%s:%s@tcp(%s:%d)/?charset=utf8&interpolateParams=true&readTimeout=%s",
		cfg.User,
		cfg.Password,
		cfg.Host,
		cfg.Port,
	)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return db, nil
}
