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

type mysqlCreator struct {
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
func (c mysqlCreator) Create(cfg *models.DBConfig) (models.DB, error) {
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

// PrepareTables implements `PrepareTables` of modes.DB
func (md *ImpMySQLDB) PrepareTables(ctx context.Context, schema string) ([]*models.Table, [][]string, error) {
	names, err := findTables(md.db, schema)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	var (
		tables      = make([]*models.Table, 0, len(names))
		columnNames = make([][]string, 0, len(names))
	)
	for _, name := range names {
		t, columnName, err := md.GetTable(ctx, schema, name)
		if err != nil {
			return nil, nil, errors.Trace(err)
		}
		tables = append(tables, t)
		columnNames = append(columnNames, columnName)
	}
	return tables, columnNames, nil
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

	md.entries = append(md.entries, key)
	md.tables[key] = t
	md.cacheColumns[key] = columns
	return t, columns, nil
}

// Insert implements `Insert` of models.DB
func (md *ImpMySQLDB) Insert(_ context.Context, schema, table string, values map[string]interface{}) error {
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
	_, err = md.db.Exec(stmt, args...)

	return errors.Trace(err)
}

// Update implements `Update` of models.DB
func (md *ImpMySQLDB) Update(_ context.Context, schema, table string, keys map[string]interface{}, values map[string]interface{}) error {
	args := make([]interface{}, 0, len(keys)+len(values))
	kvs := genSetFields(values, &args)
	where := genWhere(keys, &args)
	stmt := fmt.Sprintf("UPDATE `%s`.`%s` SET %s WHERE %s;", schema, table, kvs, where)
	_, err := md.db.Exec(stmt, args...)
	return errors.Trace(err)
}

// Delete implements `Delete` of models.DB
func (md *ImpMySQLDB) Delete(_ context.Context, schema, table string, keys map[string]interface{}) error {
	args := make([]interface{}, 0, len(keys))
	where := genWhere(keys, &args)
	stmt := fmt.Sprintf("DELETE FROM `%s`.`%s` WHERE %s;", schema, table, where)
	_, err := md.db.Exec(stmt, args...)
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
func (md *ImpMySQLDB) GenerateDML(_ context.Context, opType models.OpType) (*models.DMLParams, error) {
	if len(md.entries) == 0 {
		return nil, errors.New("ImpMySQLDB has no table cache")
	}
	entry := md.entries[rand.Intn(len(md.entries))]
	table, ok := md.tables[entry]
	if !ok {
		return nil, errors.Errorf("%s not in table cache", entry)
	}
	var (
		params *models.DMLParams
		err    error
	)
	switch opType {
	case models.Insert:
		params, err = md.genInsertSQL(table)
	case models.Update:
		params, err = md.genUpdateSQL(table)
	case models.Delete:
		params, err = md.genDeleteSQL(table)
	default:
		return nil, errors.NotValidf("DML OpType: %d", opType)
	}
	if err != nil {
		return nil, errors.Trace(err)
	}
	return params, nil
}

func (md *ImpMySQLDB) genInsertSQL(table *models.Table) (*models.DMLParams, error) {
	id, err := getMaxID(md.db, table.Schema, table.Name)
	if err != nil {
		return nil, errors.Trace(err)
	}
	id++
	keys := map[string]interface{}{
		"id": id,
	}
	values := map[string]interface{}{
		"id": id,
	}
	for _, column := range table.Columns {
		if column.Name == "id" {
			continue
		}
		values[column.Name], err = genRandomValue(column)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	params := &models.DMLParams{
		Schema: table.Schema,
		Table:  table.Name,
		Keys:   keys,
		Values: values,
	}
	return params, nil
}

func (md *ImpMySQLDB) genUpdateSQL(table *models.Table) (*models.DMLParams, error) {
	id, err := getRandID(md.db, table.Schema, table.Name)
	if err != nil {
		return nil, errors.Trace(err)
	}
	keys := map[string]interface{}{
		"id": id,
	}
	var column *models.Column
	// FIXME: better column selection and way to avoid infinite loop
	for {
		column = table.Columns[rand.Intn(len(table.Columns))]
		if column.Name != "id" && column.Key != "PRI" && column.Key != "UNI" {
			break
		}
	}
	value, err := genRandomValue(column)
	if err != nil {
		return nil, errors.Trace(err)
	}
	values := map[string]interface{}{
		column.Name: value,
	}

	params := &models.DMLParams{
		Schema: table.Schema,
		Table:  table.Name,
		Keys:   keys,
		Values: values,
	}
	return params, nil
}

func (md *ImpMySQLDB) genDeleteSQL(table *models.Table) (*models.DMLParams, error) {
	id, err := getRandID(md.db, table.Schema, table.Name)
	if err != nil {
		return nil, errors.Trace(err)
	}
	keys := map[string]interface{}{
		"id": id,
	}
	params := &models.DMLParams{
		Schema: table.Schema,
		Table:  table.Name,
		Keys:   keys,
	}
	return params, nil
}

func init() {
	models.RegisterDBCreator("mysql", mysqlCreator{})
	models.RegisterDBCreator("mariadb", mysqlCreator{})
}
