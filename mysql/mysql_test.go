package mysql

import (
	"fmt"
	"log"
	"math/rand"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/auho/go-simple-db/simple"
)

var dsn = "test:test@tcp(127.0.0.1:3306)/test"
var tableName = "test_mysql"
var lastId int64 = 0
var db *Mysql

func TestMain(m *testing.M) {
	setup()
	code := m.Run()
	tearDown()
	os.Exit(code)
}

func setup() {
	db = NewMysql(dsn)
	err := db.Connection()
	if err != nil {
		log.Fatalln(err)
	}

	err = db.Drop(tableName)
	if err != nil {
		log.Fatalln(err)
	}

	query := "CREATE TABLE IF NOT EXISTS `" + tableName + "` (" +
		"	`id` int(11) unsigned NOT NULL AUTO_INCREMENT," +
		"	`name` varchar(32) NOT NULL DEFAULT ''," +
		"	`value` int(11) NOT NULL DEFAULT '0'," +
		"	PRIMARY KEY (`id`)" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;"

	_, err = db.Exec(query)
	if err != nil {
		log.Fatalln(err)
	}
}

func tearDown() {
	err := db.Truncate(tableName)
	if err != nil {
		log.Fatalln(err)
	}

	err = db.Drop(tableName)
	if err != nil {
		log.Fatalln(err)
	}

	db.Close()
}

func Test_NewDriver(t *testing.T) {
	mysql, err := simple.NewDriver("mysql", dsn)
	if err != nil {
		t.Error(err)
	}

	err = mysql.Ping()
	if err != nil {
		t.Error(err)
	}
}

func TestMysql_NewMysql(t *testing.T) {
	driver := NewMysql(dsn)

	err := driver.Connection()
	if err != nil {
		t.Error(err)
	}

	err = driver.Ping()
	if err != nil {
		t.Error(err)
	}
}

func TestMysql_NewDriver(t *testing.T) {
	driver, err := NewDriver(dsn)
	if err != nil {
		t.Error(err)
	}

	err = driver.Ping()
	if err != nil {
		t.Error(err)
	}
}

func TestMysql_Exec(t *testing.T) {
	res, err := db.InsertFromSlice(tableName, []string{"name", "value"}, []interface{}{"exec", 1})
	if err != nil {
		t.Error(err)
	}

	lastId, err = res.LastInsertId()
	if err != nil {
		t.Error(err)
	}

	if lastId <= 0 {
		t.Error("last id is error")
	}

	rows, err := res.RowsAffected()
	if err != nil {
		t.Error(err)
	}

	if rows <= 0 {
		t.Error("rows affected is error")
	}
}

func TestMysql_InsertFromMap(t *testing.T) {
	m := make(map[string]interface{})
	m["name"] = "map"
	m["value"] = time.Now().Unix()

	res, err := db.InsertFromMap(tableName, m)
	if err != nil {
		t.Error(err)
	}

	lastId, err = res.LastInsertId()
	if err != nil {
		t.Error(err)
	}

	if lastId <= 0 {
		t.Error("last id is error")
	}

	rows, err := res.RowsAffected()
	if err != nil {
		t.Error(err)
	}

	if rows <= 0 {
		t.Error("rows affected is error")
	}
}

func TestMysql_InsertFromSlice(t *testing.T) {
	res, err := db.InsertFromSlice(tableName, []string{"name", "value"}, []interface{}{"slice", time.Now().Unix()})
	if err != nil {
		t.Error(err)
	}

	lastId, err = res.LastInsertId()
	if err != nil {
		t.Error(err)
	}

	if lastId <= 0 {
		t.Error("last id is error")
	}

	rows, err := res.RowsAffected()
	if err != nil {
		t.Error(err)
	}

	if rows <= 0 {
		t.Error("rows affected is error")
	}
}

func TestMysql_BulkInsertFromSliceSlice(t *testing.T) {
	mSlice := make([][]interface{}, 0, 1000*2)

	for i := 0; i < 1000; i++ {
		mSlice = append(mSlice, []interface{}{"slice slice", time.Now().Unix()})
	}

	res, err := db.BulkInsertFromSliceSlice(tableName, []string{"name", "value"}, mSlice)
	if err != nil {
		t.Error(err)
	}

	lastId, err = res.LastInsertId()
	if err != nil {
		t.Error(err)
	}

	if lastId <= 0 {
		t.Error("last id is error")
	}

	rows, err := res.RowsAffected()
	if err != nil {
		t.Error(err)
	}

	if rows <= 0 {
		t.Error("rows affected is error")
	}
}

func TestMysql_BulkInsertFromSliceMap(t *testing.T) {
	mSlice := make([]map[string]interface{}, 0)

	for i := 0; i < 1000; i++ {
		m := make(map[string]interface{})
		m["name"] = "slice map"
		m["value"] = time.Now().Unix()
		mSlice = append(mSlice, m)
	}

	res, err := db.BulkInsertFromSliceMap(tableName, mSlice)
	if err != nil {
		t.Error(err)
	}

	lastId, err = res.LastInsertId()
	if err != nil {
		t.Error(err)
	}

	if lastId <= 0 {
		t.Error("last id is error")
	}

	rows, err := res.RowsAffected()
	if err != nil {
		t.Error(err)
	}

	if rows <= 0 {
		t.Error("rows affected is error")
	}
}

func TestMysql_QueryInterface(t *testing.T) {
	rowsNum := rand.Intn(50) + 1
	query := fmt.Sprintf("SELECT * FROM `%s` LIMIT %d", tableName, rowsNum)

	rows, err := db.QueryInterface(query)
	if err != nil {
		t.Error(err)
	}

	if len(rows) != rowsNum {
		t.Error(fmt.Sprintf("rows num is %d, not %d", len(rows), rowsNum))
	}
}

func TestMysql_QueryInterfaceRow(t *testing.T) {
	query := fmt.Sprintf("SELECT * FROM `%s` WHERE `id` = ? LIMIT 1", tableName)

	row, err := db.QueryInterfaceRow(query, lastId)
	if err != nil {
		t.Error(err)
	}

	id := row["id"].(int64)
	if id != lastId {
		t.Error("id != last id")
	}
}

func TestMysql_QueryString(t *testing.T) {
	rowsNum := rand.Intn(50) + 1
	query := fmt.Sprintf("SELECT * FROM `%s` LIMIT %d", tableName, rowsNum)

	rows, err := db.QueryString(query)
	if err != nil {
		t.Error(err)
	}

	if len(rows) != rowsNum {
		t.Error(fmt.Sprintf("rows num is %d, not %d", len(rows), rowsNum))
	}
}

func TestMysql_QueryStringRow(t *testing.T) {
	query := fmt.Sprintf("SELECT * FROM `%s` WHERE `id` = ? LIMIT 1", tableName)

	row, err := db.QueryStringRow(query, lastId)
	if err != nil {
		t.Error(err)
	}

	id, err := strconv.ParseInt(row["id"], 10, 64)
	if err != nil {
		t.Error(err)
	}

	if id != lastId {
		t.Error("id != last id")
	}
}

func TestMysql_QueryFieldInterfaceSlice(t *testing.T) {
	rowsNum := rand.Intn(50) + 1
	minId := rand.Int63n(lastId - int64(rowsNum))
	query := fmt.Sprintf("SELECT * FROM `%s` WHERE `id` > ? LIMIT %d", tableName, rowsNum)

	values, err := db.QueryFieldInterfaceSlice("value", query, minId)
	if err != nil {
		t.Error(err)
	}

	if len(values) != rowsNum {
		t.Error("rows num is error")
	}

	if values[0].(int64) <= 0 {
		t.Error("value is error")
	}
}

func TestMysql_QueryFieldInterface(t *testing.T) {
	minId := rand.Int63n(lastId)
	query := fmt.Sprintf("SELECT * FROM `%s` WHERE `id` = ?", tableName)
	value, err := db.QueryFieldInterface("value", query, minId)
	if err != nil {
		t.Error(err)
	}

	if value.(int64) <= 0 {
		t.Error("value is error")
	}
}

func TestMysql_UpdateFromMapById(t *testing.T) {
	m := make(map[string]interface{})
	m["name"] = "update"
	m["value"] = time.Now().Unix() - 100
	m["id"] = lastId

	err := db.UpdateFromMapById(tableName, "id", m)
	if err != nil {
		t.Error(err)
	}

	query := fmt.Sprintf("SELECT * FROM `%s` WHERE `id` = ?", tableName)
	row, err := db.QueryInterfaceRow(query, lastId)
	if err != nil {
		t.Error(err)
	}

	id := row["id"].(int64)
	if id != lastId {
		t.Error("id != last id")
	}

	name := string(row["name"].([]uint8))
	if m["name"].(string) != name {
		t.Error(fmt.Sprintf("name is %s, not %s", name, m["name"]))
	}

	value := row["value"].(int64)
	if m["value"].(int64) != value {
		t.Error(fmt.Sprintf("value is %s, not %s", name, m["value"]))
	}
}

func TestMysql_BulkUpdateFromSliceMapById(t *testing.T) {
	name := "bulk update"
	rowsNum := rand.Intn(50) + 1
	mSlice := make([]map[string]interface{}, rowsNum, rowsNum)

	for i := 0; i < rowsNum; i++ {
		m := make(map[string]interface{})
		m["name"] = name
		m["value"] = time.Now().Unix() % 10e7
		m["id"] = i + 1
		mSlice[i] = m
	}

	err := db.BulkUpdateFromSliceMapById(tableName, "id", mSlice)
	if err != nil {
		t.Error(err)
	}

	query := fmt.Sprintf("SELECT * FROM `%s` WHERE `name` = ?", tableName)
	rows, err := db.QueryFieldInterfaceSlice("value", query, name)
	if err != nil {
		t.Error(err)
	}

	if len(rows) != rowsNum {
		t.Error("rows num is error")
	}

	if rows[0].(int64) <= 0 {
		t.Error("value is error")
	}
}
