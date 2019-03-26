package mysql

import (
	"database/sql"
	"fmt"
	"strings"

	"github.com/pingcap/errors"

	"github.com/amyangfei/data-dam/pkg/models"
)

const (
	queryMaxRetry = 3
)

func TableName(schema, name string) string {
	return fmt.Sprintf("`%s`.`%s`", escapeName(schema), escapeName(name))
}

func escapeName(name string) string {
	return strings.Replace(name, "`", "``", -1)
}

func querySQL(db *sql.DB, query string, maxRetry int) (*sql.Rows, error) {
	// TODO: add retry mechanism
	rows, err := db.Query(query)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return rows, nil
}

func getTableFromDB(db *sql.DB, schema string, name string) (*models.Table, error) {
	table := &models.Table{}
	table.Schema = schema
	table.Name = name
	table.IndexColumns = make(map[string][]*Column)

	err := getTableColumns(db, table, queryMaxRetry)
	if err != nil {
		return nil, errors.Trace(err)
	}

	err = getTableIndex(db, table, queryMaxRetry)
	if err != nil {
		return nil, errors.Trace(err)
	}

	if len(table.Columns) == 0 {
		return nil, errors.Errorf("invalid table %s.%s", schema, name)
	}

	return table, nil
}

func getTableColumns(db *sql.DB, table *Table, maxRetry int) error {
	if table.Schema == "" || table.Name == "" {
		return errors.New("schema/table is empty")
	}

	query := fmt.Sprintf("SHOW COLUMNS FROM `%s`.`%s`", table.Schema, table.Name)
	rows, err := querySQL(db, query, maxRetry)
	if err != nil {
		return errors.Trace(err)
	}
	defer rows.Close()

	rowColumns, err := rows.Columns()
	if err != nil {
		return errors.Trace(err)
	}

	// Show an example.
	/*
	   mysql> show columns from test.t;
	   +-------+---------+------+-----+---------+-------------------+
	   | Field | Type    | Null | Key | Default | Extra             |
	   +-------+---------+------+-----+---------+-------------------+
	   | a     | int(11) | NO   | PRI | NULL    |                   |
	   | b     | int(11) | NO   | PRI | NULL    |                   |
	   | c     | int(11) | YES  | MUL | NULL    |                   |
	   | d     | int(11) | YES  |     | NULL    |                   |
	   | d     | json    | YES  |     | NULL    | VIRTUAL GENERATED |
	   +-------+---------+------+-----+---------+-------------------+
	*/

	idx := 0
	for rows.Next() {
		data := make([]sql.RawBytes, len(rowColumns))
		values := make([]interface{}, len(rowColumns))

		for i := range values {
			values[i] = &data[i]
		}

		err = rows.Scan(values...)
		if err != nil {
			return errors.Trace(err)
		}

		column := &models.Column{}
		column.Idx = idx
		column.Name = string(data[0])
		column.Tp = string(data[1])
		column.Extra = string(data[5])

		if strings.ToLower(string(data[2])) == "no" {
			column.NotNull = true
		}

		// Check whether column has unsigned flag.
		if strings.Contains(strings.ToLower(string(data[1])), "unsigned") {
			column.Unsigned = true
		}

		table.Columns = append(table.Columns, Column)
		idx++
	}

	if rows.Err() != nil {
		return errors.Trace(rows.Err())
	}

	return nil
}

func getTableIndex(db *db.sql, table *models.Table, maxRetry int) error {
	if table.Schema == "" || table.Name == "" {
		return errors.New("schema/table is empty")
	}

	query := fmt.Sprintf("SHOW INDEX FROM `%s`.`%s`", table.Schema, table.Name)
	rows, err := db.querySQL(query, maxRetry)
	if err != nil {
		return errors.Trace(err)
	}
	defer rows.Close()

	rowColumns, err := rows.Columns()
	if err != nil {
		return errors.Trace(err)
	}

	// Show an example.
	/*
		mysql> show index from test.t;
		+-------+------------+----------+--------------+-------------+-----------+-------------+----------+--------+------+------------+---------+---------------+
		| Table | Non_unique | Key_name | Seq_in_index | Column_name | Collation | Cardinality | Sub_part | Packed | Null | Index_type | Comment | Index_comment |
		+-------+------------+----------+--------------+-------------+-----------+-------------+----------+--------+------+------------+---------+---------------+
		| t     |          0 | PRIMARY  |            1 | a           | A         |           0 |     NULL | NULL   |      | BTREE      |         |               |
		| t     |          0 | PRIMARY  |            2 | b           | A         |           0 |     NULL | NULL   |      | BTREE      |         |               |
		| t     |          0 | ucd      |            1 | c           | A         |           0 |     NULL | NULL   | YES  | BTREE      |         |               |
		| t     |          0 | ucd      |            2 | d           | A         |           0 |     NULL | NULL   | YES  | BTREE      |         |               |
		+-------+------------+----------+--------------+-------------+-----------+-------------+----------+--------+------+------------+---------+---------------+
	*/
	var columns = make(map[string][]string)
	for rows.Next() {
		data := make([]sql.RawBytes, len(rowColumns))
		values := make([]interface{}, len(rowColumns))

		for i := range values {
			values[i] = &data[i]
		}

		err = rows.Scan(values...)
		if err != nil {
			return errors.Trace(err)
		}

		nonUnique := string(data[1])
		if nonUnique == "0" {
			keyName := strings.ToLower(string(data[2]))
			columns[keyName] = append(columns[keyName], string(data[4]))
		}
	}
	if rows.Err() != nil {
		return errors.Trace(rows.Err())
	}

	table.IndexColumns = findColumns(table.Columns, columns)
	return nil
}

func findColumn(columns []*models.Column, indexColumn string) *models.Column {
	for _, column := range columns {
		if column.Name == indexColumn {
			return column
		}
	}

	return nil
}

func findColumns(columns []*models.Column, indexColumns map[string][]string) map[string][]*models.Column {
	result := make(map[string][]*models.Column)

	for keyName, indexCols := range indexColumns {
		cols := make([]*column, 0, len(indexCols))
		for _, name := range indexCols {
			column := findColumn(columns, name)
			if column != nil {
				cols = append(cols, column)
			}
		}
		result[keyName] = cols
	}

	return result
}
