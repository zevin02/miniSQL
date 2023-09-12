package lexer

import (
	"fmt"
	"testing"
)

func TestLexer(t *testing.T) {
	source := "if a >= 100.34"
	myLexer := NewLexer(source)
	for {
		token, err := myLexer.Scan()
		if err != nil {
			fmt.Println("lexer error:", err)
			break
		}
		if token.tag == EOF {
			break //读取结束了
		} else {
			fmt.Println("read token ", token)
		}

	}
}
