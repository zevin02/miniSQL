package lexer

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestLexer1(t *testing.T) {
	source := "if a >= 100.34"
	myLexer := NewLexer(source)
	for {
		token, err := myLexer.Scan()
		if err != nil {
			fmt.Println("lexer error:", err)
			break
		}
		if token.Tag == EOF {
			break //读取结束了
		} else {
			fmt.Println("read token ", token)
		}

	}
}

func TestLexer2(t *testing.T) {
	//关键词Select，FROM，WHERE，selectList，TABLELIST PREDICATE（判断条件）
	//select selectlist from tablelist where predicate
	//predicate->term (and predicate),条件里面包含条件
	//query->select selectlist from tablelist (where predicate)
	//tablelist->ID(COMMA,TABLELIST)

	NewLexer("select age, name, sex from student, department where age =20 and sex =\"male\"")

}

func TestLexer3(t *testing.T) {
	sqlLexer := NewLexer("SELECT age, sex FROM student WHERE age > 20 AND sex = \"male\"")
	//
	/*
		select ->SELECT
		name sex ->ID
		from ->FROM
		where->WHERE
		> ->GREATER_OPERATION
		20 ->NUM
	*/
	var token []*Token //先把这些token创建出来
	token = append(token, NewTokenWithString(SELECT, "SELECT"))
	token = append(token, NewTokenWithString(ID, "age"))
	token = append(token, NewTokenWithString(COMMA, ","))
	token = append(token, NewTokenWithString(ID, "sex"))
	token = append(token, NewTokenWithString(FROM, "FROM"))
	token = append(token, NewTokenWithString(ID, "student"))
	token = append(token, NewTokenWithString(WHERE, "WHERE"))
	token = append(token, NewTokenWithString(ID, "age"))
	token = append(token, NewTokenWithString(GREATER_OPERATOR, ">"))
	token = append(token, NewTokenWithString(NUM, "20"))
	token = append(token, NewTokenWithString(AND, "AND"))
	token = append(token, NewTokenWithString(ID, "sex"))
	token = append(token, NewTokenWithString(ASSIGN_OPERATOR, "="))
	token = append(token, NewTokenWithString(STRING, "\"male\""))

	for _, tok := range token {
		sqlTok, err := sqlLexer.Scan()
		if err != nil {
			fmt.Println("lexer error")
		}
		assert.Equal(t, tok.Tag, sqlTok.Tag)
	}
}
