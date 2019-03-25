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

type mysqlCreator struct{}

type mysqlDB struct {
	dbs     []*sql.DB
	verbose bool
}

func (c mysqlCreator) Create(concurrent int, cfg models.DBConfig, verbose bool) (models.DB, error) {
	d := &mysqlDB{
		verbose: verbose,
	}
	dbs := make([]*sql.DB, 0, concurrent+1)
	for i := 0; i < concurrent+1; i++ {
		db, err := createDB(cfg.MySQL)
		if err != nil {
			closeDBs(dbs)
			return nil, errors.Trace(err)
		}
	}
	c.dbs = dbs
	return c, nil
}

func closeDBs(dbs ...*sql.DB) {
	for _, db := range dbs {
		if db != nil {
			err := db.Close()
			if err != nil {
				log.Errorf("close db failed: %v", err)
			}
		}
	}
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
