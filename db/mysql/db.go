package mysql

import (
	"bytes"
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

	tables       map[string]*models.Table // table cache: `target-schema`.`target-table` -> table
	cacheColumns map[string][]string      // table columns cache: `target-schema`.`target-table` -> column names list
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
		verbose:      cfg.Verbose,
		tables:       make(map[string]*models.Table),
		cacheColumns: make(map[string][]string),
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

func (md *MySQLDB) clearTableCache(schema, table string) {
	key := TableName(schema, table)
	delete(md.tables, key)
	delete(md.cacheColumns, key)
}

func (md *MySQLDB) clearAllTableCache() {
	md.tables = make(map[string]*models.Table)
	md.cacheColumns = make(map[string][]string)
}

func genSetFields(values map[string]interface{}, args *[]interface{}) string {
	var (
		buf strings.Builder
		idx = 0
	)
	for k, v := range keys {
		if idx == len(keys)-1 {
			fmt.Fprintf(&buf, "`%s` = ?", k)
		} else {
			fmt.Fprintf(&buf, "`%s` = ?, ", k)
		}
		*args = append(*args, v)
		idx++
	}
	return buf.String()
}

func genWhere(keys map[string]interface{}, args *[]interface{}) string {
	var (
		buf strings.Builder
		idx = 0
	)
	for k, v := range keys {
		kvSplit := "="
		if v == nil {
			kvSplit = "IS"
		}
		if idx == len(keys)-1 {
			fmt.Fprintf(&buf, "`%s` %s ?", k, kvSplit)
		} else {
			fmt.Fprintf(&buf, "`%s` %s ? AND ", k, kvSplit)
		}
		*args = append(*args, v)
		idx++
	}
	return buf.String()
}

// GetTable implements `GetTable` of models.DB
func (md *MySQLDB) GetTable(ctx context.Context, schema, table string) (*models.Table, []string, error) {
	key := TableName(schema, table)

	value, ok := md.tables[key]
	if ok {
		return value, md.cacheColumns[key], nil
	}

	t, err := getTableFromDB(md.db, schema, table)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}

	// compute cache column list for column mapping
	columns := make([]string, 0, len(t.Columns))
	for _, c := range t.Columns {
		columns = append(columns, c.name)
	}

	md.tables[key] = t
	md.cacheColumns[key] = columns
	return t, columns, nil
}

// Insert implements `Insert` of models.DB
func (md *MySQLDB) Insert(ctx context.Context, schema, table string, values map[string]interface{}) error {
	var (
		args        = make([]interface{}, 0, len(values))
		buf, valbuf strings.Builder
		idx         = 0
		err         error
	)
	for k, v := range values {
		if idx == len(values)-1 {
			fmt.Fprintf(&buf, "`%s`")
			fmt.Fprintf(&valbuf, "?")
		} else {
			fmt.Fprintf(&buf, "`%s`, ")
			fmt.Fprintf(&valbuf, "?, ")
		}
		args = append(args, v)
		idx++
	}
	stmt := fmt.Sprintf("INSERT INTO `%s`.`%s` (%s) VALUES (%s);", schema, table, buf.String(), valbuf.String())
	_, err = md.db.ExecContext(ctx, stmt, args...)

	return errors.Trace(err)
}

// Update implements `Update` of models.DB
func (md *MySQLDB) Update(ctx context.Context, schema, table string, keys map[string]interface{}, values map[string]interface{}) error {
	args := make([]interface{}, 0, len(keys)+len(values))
	kvs := genSetFields(values, &args)
	where := genWhere(keys, &args)
	stmt := fmt.Sprintf("UPDATE `%s`.`%s` SET %s WHERE %s;", schema, table, kvs, where)
	_, err = md.db.ExecContext(ctx, stmt, args...)
	return errors.Trace(err)
}

// Delete implements `Delete` of models.DB
func (md *MySQLDB) Delete(ctx context.Context, schema, table string, keys map[string]interface{}) error {
	args := make([]interface{}, 0, len(keys))
	where := genWhere(keys, &args)
	stmt := fmt.Sprintf("DELETE FROM `%s`.`%s` WHERE %s;", schema, table, where)
	_, err = md.db.ExecContext(ctx, stmt, args...)
	return errors.Trace(err)
}
