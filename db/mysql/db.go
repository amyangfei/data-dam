package mysql

import (
	"database/sql"
	"fmt"
	"strings"

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

// Create implements `Create` of models.DB
func (md *MySQLDB) Create(cfg models.DBConfig) (models.DB, error) {
	md := &MySQLDB{
		verbose: cfg.Verbose,
	}
	db, err := createDB(cfg.MySQL)
	if err != nil {
		if db != nil {
			db.Close()
		}
		return nil, errors.Trace(err)
	}
	md.db = db
	return md, nil
}

// Insert implements `Insert` of models.DB
func (md *MySQLDB) Insert(ctx context.Context, schema, table string, values map[string]interface{}) error {
	var (
		fields, placeholder = make([]string, 0, len(values))
		args                = make([]intervace{}, 0, len(values))
		err                 error
	)
	for k, v := range values {
		fields = append(fields, "`"+k+"`")
		placeholder = append(placeholder, "?")
		args = append(args, v)
	}
	stmt := fmt.Sprintf(
		"INSERT INTO `%s`.`%s` (%s) VALUES (%s)", schema, table,
		strings.Join(fields, ","), strings.Join(placeholder))
	_, err = md.db.ExecContext(ctx, stmt, args...)

	return errors.Trace(err)
}

// Insert implements `Update` of models.DB
func (md *MySQLDB) Update(ctx context.Context, schema, table string, keys map[string]interface{}, values map[string]interface{}) error {
}

// Insert implements `Delete` of models.DB
func (md *MySQLDB) Delete(ctx context.Context, schema, table string, keys map[string]interface{}) error {
}
