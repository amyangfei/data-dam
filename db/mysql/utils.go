package mysql

import (
	"database/sql"
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"time"
	"unsafe"

	"github.com/pingcap/errors"

	"github.com/amyangfei/data-dam/pkg/models"
)

const (
	queryMaxRetry = 3
	letterBytes   = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	letterIdxBits = 6                    // 6 bits to represent a letter index
	letterIdxMask = 1<<letterIdxBits - 1 // All 1-bits, as many as letterIdxBits
	letterIdxMax  = 63 / letterIdxBits   // # of letter indices fitting in 63 bits
)

// TableName returns table name with schema
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
	table.IndexColumns = make(map[string][]*models.Column)

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

func getTableColumns(db *sql.DB, table *models.Table, maxRetry int) error {
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
		column.Key = string(data[3])
		column.Extra = string(data[5])
		bracketIdx := strings.Index(column.Tp, "(")
		if bracketIdx > 0 {
			column.SubTp = column.Tp[bracketIdx+1 : len(column.Tp)-1]
			column.Tp = column.Tp[:bracketIdx]
		}

		if strings.ToLower(string(data[2])) == "no" {
			column.NotNull = true
		}

		// Check whether column has unsigned flag.
		if strings.Contains(strings.ToLower(string(data[1])), "unsigned") {
			column.Unsigned = true
		}

		table.Columns = append(table.Columns, column)
		idx++
	}

	if rows.Err() != nil {
		return errors.Trace(rows.Err())
	}

	return nil
}

func getTableIndex(db *sql.DB, table *models.Table, maxRetry int) error {
	if table.Schema == "" || table.Name == "" {
		return errors.New("schema/table is empty")
	}

	query := fmt.Sprintf("SHOW INDEX FROM `%s`.`%s`", table.Schema, table.Name)
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
		cols := make([]*models.Column, 0, len(indexCols))
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

func findTables(db *sql.DB, schema string) ([]string, error) {
	query := fmt.Sprintf("SHOW TABLES FROM `%s`", schema)
	rows, err := querySQL(db, query, queryMaxRetry)
	if err != nil {
		return nil, errors.Trace(err)
	}
	defer rows.Close()

	tables := make([]string, 0)
	for rows.Next() {
		var table string
		err = rows.Scan(&table)
		if err != nil {
			return nil, errors.Trace(err)
		}
		tables = append(tables, table)
	}

	if rows.Err() != nil {
		return nil, errors.Trace(rows.Err())
	}

	return tables, nil
}

func getMaxID(db *sql.DB, schema, table string) (int64, error) {
	stmt := fmt.Sprintf("SELECT IFNULL(max(id), 0) FROM `%s`.`%s`", schema, table)
	rows, err := db.Query(stmt)
	if err != nil {
		return 0, errors.Trace(err)
	}
	defer rows.Close()
	var id int64
	for rows.Next() {
		if err := rows.Scan(&id); err != nil {
			return 0, errors.Trace(err)
		}
	}
	return id, nil
}

func getRandID(db *sql.DB, schema, table string) (int, error) {
	stmt := fmt.Sprintf("SELECT IFNULL(id, 0) FROM `%s`.`%s` ORDER BY RAND() LIMIT 1", schema, table)
	rows, err := db.Query(stmt)
	if err != nil {
		return 0, errors.Trace(err)
	}
	defer rows.Close()
	var id int
	for rows.Next() {
		if err := rows.Scan(&id); err != nil {
			return 0, errors.Trace(err)
		}
	}
	return id, nil
}

func genRandomValue(column *models.Column) (interface{}, error) {
	booleans := []string{"TRUE", "FALSE"}
	upper := strings.ToUpper(column.Tp)
	var value interface{}
	switch upper {
	case "INT":
		value = rand.Int31()
	case "INTUNSIGNED":
		value = rand.Int31()
	case "BOOLEAN":
		value = booleans[rand.Intn(len(booleans))]
	case "BIGINT":
		value = rand.Int63()
	case "BIGINTUNSIGNED":
		value = rand.Int63()
	case "DOUBLE":
		value = rand.ExpFloat64()
	case "DOUBLEUNSIGNED":
		value = rand.ExpFloat64()
	case "DECIMAL":
		value = strconv.FormatFloat(rand.ExpFloat64(), 'f', 5, 64)
	case "DATETIME", "TIMESTAMP", "TIMESTAMPONUPDATE":
		t := genRandomTime()
		value = fmt.Sprintf("%.4d-%.2d-%.2d %.2d:%.2d:%.2d", t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), t.Second())
	case "TIME":
		t := genRandomTime()
		value = fmt.Sprintf("%.2d:%.2d:%.2d", t.Hour(), t.Minute(), t.Second())
	case "YEAR":
		t := genRandomTime()
		value = fmt.Sprintf("%.4d", t.Year())
	case "CHAR":
		n, err := strconv.Atoi(column.SubTp)
		if err != nil {
			return nil, errors.Trace(err)
		}
		value = genRandStringBytesMaskImprSrcUnsafe(n)
	case "VARCHAR":
		n, err := strconv.Atoi(column.SubTp)
		if err != nil {
			return nil, errors.Trace(err)
		}
		value = genRandStringBytesMaskImprSrcUnsafe(rand.Intn(n) + 1)
	case "BLOB":
		value = genRandomByteString(20)
	case "TEXT":
		value = genRandomUnicodeString(20)
	case "ENUM":
		candidates := strings.Split(column.SubTp, ",")
		val := candidates[rand.Intn(len(candidates))]
		val = val[1 : len(val)-1]
		value = val
	case "SET":
		candidates := strings.Split(column.SubTp, ",")
		s := make([]string, 0, len(candidates))
		for _, candidate := range candidates {
			if rand.Intn(2) == 0 {
				s = append(s, candidate[1:len(candidate)-1])
			}
		}
		value = strings.Join(s, ",")
	}
	return value, nil
}

func genRandomTime() time.Time {
	min := time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC).Unix()
	max := time.Date(2037, 12, 31, 0, 0, 0, 0, time.UTC).Unix()
	delta := max - min
	sec := rand.Int63n(delta) + min
	return time.Unix(sec, 0)
}

// https://stackoverflow.com/a/31832326/1115857
func genRandStringBytesMaskImprSrcUnsafe(n int) string {
	b := make([]byte, n)
	// A src.Int63() generates 63 random bits, enough for letterIdxMax characters!
	for i, cache, remain := n-1, rand.Int63(), letterIdxMax; i >= 0; {
		if remain == 0 {
			cache, remain = rand.Int63(), letterIdxMax
		}
		if idx := int(cache & letterIdxMask); idx < len(letterBytes) {
			b[i] = letterBytes[idx]
			i--
		}
		cache >>= letterIdxBits
		remain--
	}

	return *(*string)(unsafe.Pointer(&b))
}

func genRandomUnicodeString(n int) string {
	var builder strings.Builder
	builder.Grow(2 + 3*n)
	builder.WriteByte('\'')
	for i := 0; i < n; i++ {
		// 50% chance generating ASCII string, 50% chance generating Unicode string
		var r rune
		switch rand.Intn(2) {
		case 0:
			r = rune(rand.Intn(0x80))
		case 1:
			r = rune(rand.Intn(0xd800))
		}
		switch r {
		case '\'':
			builder.WriteString("''")
		case '\\':
			builder.WriteString(`\\`)
		default:
			builder.WriteRune(r)
		}
	}
	builder.WriteByte('\'')
	return builder.String()
}

func genRandomByteString(n int) string {
	var builder strings.Builder
	builder.Grow(3 + 2*n)
	builder.WriteString("x'")
	for i := 0; i < n; i++ {
		fmt.Fprintf(&builder, "%02X", rand.Intn(256))
	}
	builder.WriteString("'")
	return builder.String()
}
