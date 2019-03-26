package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"math/rand"
	"strings"

	"github.com/pingcap/errors"

	"github.com/amyangfei/data-dam/pkg/models"
)

const (
	defaultTimeout = "3s"
)

// ImpMySQLDB implements models.DB
type ImpMySQLDB struct {
	db      *sql.DB
	verbose bool

	entries      []string                 // table name cache: a `schema`.`table` slice
	tables       map[string]*models.Table // table cache: `schema`.`table` -> table
	cacheColumns map[string][]string      // table columns cache: `schema`.`table` -> column names list
	nextID       int64
}

func createDB(cfg models.MySQLConfig) (*sql.DB, error) {
	dsn := fmt.Sprintf(
		"%s:%s@tcp(%s:%d)/?charset=utf8&interpolateParams=true&readTimeout=%s",
		cfg.User,
		cfg.Password,
		cfg.Host,
		cfg.Port,
		defaultTimeout,
	)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return db, nil
}

// Create creates a models.DB
func Create(cfg models.DBConfig) (models.DB, error) {
	md := &ImpMySQLDB{
		verbose:      cfg.Verbose,
		entries:      make([]string, 0),
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

func (md *ImpMySQLDB) clearTableCache(schema, table string) {
	key := TableName(schema, table)
	for i := range md.entries {
		if md.entries[i] == key {
			md.entries = append(md.entries[:i], md.entries[i+1:]...)
			break
		}
	}
	delete(md.tables, key)
	delete(md.cacheColumns, key)
}

func (md *ImpMySQLDB) clearAllTableCache() {
	md.entries = make([]string, 0)
	md.tables = make(map[string]*models.Table)
	md.cacheColumns = make(map[string][]string)
}

func genSetFields(values map[string]interface{}, args *[]interface{}) string {
	var (
		buf strings.Builder
		idx = 0
	)
	for k, v := range values {
		if idx == len(values)-1 {
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
func (md *ImpMySQLDB) GetTable(ctx context.Context, schema, table string) (*models.Table, []string, error) {
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
		columns = append(columns, c.Name)
	}

	md.tables[key] = t
	md.cacheColumns[key] = columns
	return t, columns, nil
}

// Insert implements `Insert` of models.DB
func (md *ImpMySQLDB) Insert(ctx context.Context, schema, table string, values map[string]interface{}) error {
	var (
		args        = make([]interface{}, 0, len(values))
		buf, valbuf strings.Builder
		idx         = 0
		err         error
	)
	for k, v := range values {
		if idx == len(values)-1 {
			fmt.Fprintf(&buf, "`%s`", k)
			fmt.Fprintf(&valbuf, "?")
		} else {
			fmt.Fprintf(&buf, "`%s`, ", k)
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
func (md *ImpMySQLDB) Update(ctx context.Context, schema, table string, keys map[string]interface{}, values map[string]interface{}) error {
	args := make([]interface{}, 0, len(keys)+len(values))
	kvs := genSetFields(values, &args)
	where := genWhere(keys, &args)
	stmt := fmt.Sprintf("UPDATE `%s`.`%s` SET %s WHERE %s;", schema, table, kvs, where)
	_, err := md.db.ExecContext(ctx, stmt, args...)
	return errors.Trace(err)
}

// Delete implements `Delete` of models.DB
func (md *ImpMySQLDB) Delete(ctx context.Context, schema, table string, keys map[string]interface{}) error {
	args := make([]interface{}, 0, len(keys))
	where := genWhere(keys, &args)
	stmt := fmt.Sprintf("DELETE FROM `%s`.`%s` WHERE %s;", schema, table, where)
	_, err := md.db.ExecContext(ctx, stmt, args...)
	return errors.Trace(err)
}

// Close implements `Close` of models.DB
func (md *ImpMySQLDB) Close() error {
	if md.db != nil {
		err := md.db.Close()
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

// GenerateDML implements `GenerateDML` of models.DB
func (md *ImpMySQLDB) GenerateDML(ctx context.Context, opType models.OpType) (*models.DMLParams, error) {
	if len(md.entries) == 0 {
		return nil, errors.New("ImpMySQLDB has no table cache")
	}
	entry := md.entries[rand.Intn(len(md.entries))]
	table, ok := md.tables[entry]
	if !ok {
		return nil, errors.Errorf("%s not in table cache", entry)
	}
	schema, name := table.Schema, table.Name
	switch opType {
	case models.Insert:
	case models.Update:
	case models.Delete:
	default:
		return nil, errors.NotValidf("DML OpType: %d", opType)
	}
	params := &models.DMLParams{
		Schema: schema,
		Table:  name,
		Keys:   nil,
		Values: nil,
	}
	return params, nil
}
