package parser

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestSelect(t *testing.T) {
	sqlParser := NewSQLParser("age = 20")
	term, err := sqlParser.Term() //开启解析过程
	assert.NotNil(t, term)
	assert.Nil(t, err)
	sqlParser1 := NewSQLParser("age = 20 AND name =15 AND time = file")
	pd, _ := sqlParser1.Predicate()
	assert.NotNil(t, pd)
	sqlParser2 := NewSQLParser("age,file,name from")
	fieldList := []string{"age", "file", "name"}
	sl := sqlParser2.IDList()
	assert.Equal(t, fieldList, sl)
	assert.NotNil(t, sl)
	assert.Nil(t, err)
	assert.NotNil(t, sqlParser2)
	sqlParser3 := NewSQLParser("age,file,name from")
	tl := sqlParser3.IDList()
	assert.NotNil(t, tl)
	sqlParser4 := NewSQLParser("SELECT AGE,NAME,DATE FROM T,B WHERE AGE = 1 AND TIME = \"AGE\" AND DATE =12")
	qd, err := sqlParser4.Query()
	fmt.Println(qd.ToString())
	sqlParser5 := NewSQLParser("SELECT AGE,NAME,DATE FROM T,B")
	qd1, err := sqlParser5.Query()
	fmt.Println(qd1.ToString())
	assert.Nil(t, err)
}

//这个测试测试所有有问题的SQL语句
func TestErrorSyntax(t *testing.T) {
	sqlParser1 := NewSQLParser("Selec AGE,NAME,DATE FROM T,B WHERE AGE = 1 AND TIME = \"AGE\" AND DATE =12")
	_, err := sqlParser1.Query()
	assert.NotNil(t, err)
	sqlParser2 := NewSQLParser("CReate TABLa PERSRON (PERSONID INT, LASTNAME VARCHAR(255),FIRSTNAME VARCHAR(255),ADDRESS INT)")
	_, err = sqlParser2.UpdateCmd()
	assert.NotNil(t, err)
	sqlParser3 := NewSQLParser("CREATE VIEW employeeview AS SELEC employeeid, firstname, lastname, salary FROM employees WHER salary = 50000")
	_, err = sqlParser3.UpdateCmd()
	assert.NotNil(t, err)
	sqlParser4 := NewSQLParser("Select AGE,NAME,DATE FROM T,B WHERa AGE = 1 AND TIME = \"AGE\" AND DATE =12")
	_, err = sqlParser4.Query()
	assert.NotNil(t, err)
}

func TestCreate(t *testing.T) {
	sql := "CREATE TABLE PERSRON (PERSONID INT, LASTNAME VARCHAR(255),FIRSTNAME VARCHAR(255),ADDRESS INT)"
	parser := NewSQLParser(sql)
	tbdt, _ := parser.UpdateCmd()
	i := tbdt.(*CreateTableData) //将该接口类型进行类型转化
	assert.NotNil(t, i)
	assert.Equal(t, "PERSRON", i.tableName)
	fields := i.schema.Fields()
	expectfield := []string{"PERSONID", "LASTNAME", "FIRSTNAME", "ADDRESS"}
	assert.Equal(t, expectfield, fields)
}

func TestInsert(t *testing.T) {
	//sql := "INSERT INTO PERSON (NAME,ID) VALUES (1,\"20\")"
	sql := "INSERT INTO Customers (CustomerName, ContactName, Address, City, PostalCode, Country) " +
		"VALUES (\"Cardinal\", \"Tom B. Erichsen\", \"Skagen 21\", \"Stavanger\", 4006, \"Norway\")"
	parser := NewSQLParser(sql)
	it, _ := parser.UpdateCmd()
	it = it.(*InsertData)
}

func TestView(t *testing.T) {
	//TODO 解析字符串的时候，需要把字符串中间的下划线包括进去
	sql := "CREATE VIEW employeeview AS SELECT employeeid, firstname, lastname, salary FROM employees WHERE salary = 50000"
	parser := NewSQLParser(sql)
	it, _ := parser.UpdateCmd()
	dd := it.(*CreateViewData)
	assert.NotNil(t, dd)
	fmt.Println(dd.ToString())
	sql2 := "CREATE VIEW Customer AS SELECT CustomerName, ContactName FROM customers WHERE country=\"China\""
	sqlParser := NewSQLParser(sql2)
	dit, _ := sqlParser.UpdateCmd()
	dd2 := dit.(*CreateViewData)
	assert.NotNil(t, dd2)
	fmt.Println(dd2.ToString())
}

func TestIndex(t *testing.T) {
	sql := "CREATE INDEX indexName ON PERSON (LASTNAME,FIRST)"
	parser := NewSQLParser(sql)
	it, _ := parser.UpdateCmd()
	dd := it.(*CreateIndexData)
	fmt.Println(dd.ToString())
}

func TestDelete(t *testing.T) {
	//sql := "DELETE FROM EMPLENT WHERE PERSON = 20 AND NAME = \"STU\""
	sql := "DELETE FROM EMPLENT"
	parser := NewSQLParser(sql)
	it, _ := parser.UpdateCmd()
	dd := it.(*DeleteData)
	assert.NotNil(t, dd)
}

func TestUpdate(t *testing.T) {
	sql := "UPDATE PERSON SET AGE = 20 WHERE NAME=\"NAME\" AND PRED = 10"
	parser := NewSQLParser(sql)
	it, _ := parser.UpdateCmd()
	dd := it.(*UpdateData)
	assert.NotNil(t, dd)
}
