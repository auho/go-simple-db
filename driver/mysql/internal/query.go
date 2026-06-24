package internal

import "fmt"

// SQL generates MySQL-specific SQL statements.
type SQL struct{}

// QuoteIdentifier wraps a MySQL identifier with backticks.
func (s SQL) QuoteIdentifier(name string) string {
	return fmt.Sprintf("`%s`", name)
}

func (s SQL) Truncate(table string) string {
	return fmt.Sprintf("TRUNCATE TABLE %s", s.QuoteIdentifier(table))
}

func (s SQL) Drop(table string) string {
	return fmt.Sprintf("DROP TABLE IF EXISTS %s", s.QuoteIdentifier(table))
}

func (s SQL) CopyStructure(src, dst string) string {
	return fmt.Sprintf("CREATE TABLE %s LIKE %s", s.QuoteIdentifier(dst), s.QuoteIdentifier(src))
}

func (s SQL) CopyData(src, dst string) string {
	return fmt.Sprintf("INSERT INTO %s SELECT * FROM %s", s.QuoteIdentifier(dst), s.QuoteIdentifier(src))
}

func (s SQL) RowCount(table string) string {
	return fmt.Sprintf("SELECT COUNT(*) FROM %s", s.QuoteIdentifier(table))
}

func (s SQL) GetDatabase() string {
	return "SELECT DATABASE()"
}

func (s SQL) GetTableColumnsSchema() string {
	return "SELECT `COLUMN_NAME` AS `name`, `DATA_TYPE` AS `field_type` " +
		"FROM `information_schema`.`COLUMNS` " +
		"WHERE `TABLE_SCHEMA` = ? AND `TABLE_NAME` = ?"
}

func (s SQL) GetTableColumns() string {
	return "SELECT `COLUMN_NAME` " +
		"FROM `information_schema`.`COLUMNS` " +
		"WHERE `TABLE_SCHEMA` = ? AND `TABLE_NAME` = ?"
}
