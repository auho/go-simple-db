package internal

import "fmt"

// QuoteIdentifier wraps a ClickHouse identifier with backticks.
func QuoteIdentifier(name string) string {
	return fmt.Sprintf("`%s`", name)
}

// TruncateSQL returns the SQL to truncate a table.
func TruncateSQL(table string) string {
	return fmt.Sprintf("TRUNCATE TABLE %s", QuoteIdentifier(table))
}

// DropSQL returns the SQL to drop a table if it exists.
func DropSQL(table string) string {
	return fmt.Sprintf("DROP TABLE IF EXISTS %s", QuoteIdentifier(table))
}

// CopyStructureSQL returns the SQL to copy a table structure (without data).
func CopyStructureSQL(src, dst string) string {
	return fmt.Sprintf("CREATE TABLE %s AS %s WITH NO DATA", QuoteIdentifier(dst), QuoteIdentifier(src))
}

// CopyDataSQL returns the SQL to copy data from src to dst.
func CopyDataSQL(src, dst string) string {
	return fmt.Sprintf("INSERT INTO %s SELECT * FROM %s", QuoteIdentifier(dst), QuoteIdentifier(src))
}

// RowCountSQL returns the SQL to count rows in a table.
func RowCountSQL(table string) string {
	return fmt.Sprintf("SELECT COUNT() FROM %s", QuoteIdentifier(table))
}

// GetDatabaseSQL returns the SQL to get the current database name.
const GetDatabaseSQL = "SELECT currentDatabase()"

// GetTableColumnsSchemaSQL returns the SQL to get column schemas for a table.
const GetTableColumnsSchemaSQL = "SELECT `name`, `type` AS `field_type` " +
	"FROM `system`.`columns` " +
	"WHERE `database` = ? AND `table` = ?"

// GetTableColumnsSQL returns the SQL to get column names for a table.
const GetTableColumnsSQL = "SELECT `name` " +
	"FROM `system`.`columns` " +
	"WHERE `database` = ? AND `table` = ?"
