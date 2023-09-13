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
	pd := sqlParser1.Predicate()
	assert.NotNil(t, pd)
	sqlParser2 := NewSQLParser("age,file,name from")
	fieldList := []string{"age", "file", "name"}
	sl, err := sqlParser2.SelectList()
	assert.Equal(t, fieldList, sl)
	assert.NotNil(t, sl)
	assert.Nil(t, err)
	assert.NotNil(t, sqlParser2)
	sqlParser3 := NewSQLParser("age,file,name from")
	tl := sqlParser3.TableList()
	assert.NotNil(t, tl)
	sqlParser4 := NewSQLParser("SELECT AGE,NAME,DATE FROM T,B WHERE AGE = 1 AND TIME = \"AGE\" AND DATE =12")
	qd := sqlParser4.Query()
	fmt.Println(qd.ToString())
	sqlParser5 := NewSQLParser("SELECT AGE,NAME,DATE FROM T,B")
	qd1 := sqlParser5.Query()
	fmt.Println(qd1.ToString())
}

func TestCreate(t *testing.T) {
	sql := "CREATE TABLE PERSRON (PERSONID INT, LASTNAME VARCHAR(255),FIRSTNAME VARCHAR(255),ADDRESS INT)"
	parser := NewSQLParser(sql)
	tbdt := parser.UpdateCmd()
	i := tbdt.(*TableData) //将该接口类型进行类型转化
	assert.NotNil(t, i)
	assert.Equal(t, "PERSRON", i.tableName)
	fields := i.schema.Fields()
	expectfield := []string{"PERSONID", "LASTNAME", "FIRSTNAME", "ADDRESS"}
	assert.Equal(t, expectfield, fields)
}
