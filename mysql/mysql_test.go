package mysql

import (
	"log"
	"os"
	"testing"
	"time"
)

var dsn = "test:test@tcp(127.0.0.1:3306)/test"
var tableName = "test"
var lastId int64 = 0
var db *Mysql

func TestMain(m *testing.M) {
	setup()
	code := m.Run()
	teardown()
	os.Exit(code)
}

func setup() {
	db = NewMysql(dsn)
	err := db.Connection()
	if err != nil {
		log.Fatalln(err)
	}
}

func teardown() {
	db.Close()
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

func TestMysql_UpdateFromMapById(t *testing.T) {
	m := make(map[string]interface{}, 0)
	m["name"] = "update"
	m["id"] = lastId

	err := db.UpdateFromMapById(tableName, "id", m)
	if err != nil {
		t.Error(err)
	}
}
