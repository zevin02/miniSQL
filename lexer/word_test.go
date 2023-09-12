package lexer

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestWordToken(t *testing.T) {
	word := NewWordToken("variable", ID)         //所有的字符串变量只要不是关键字，就是ID类型
	assert.Equal(t, "variable", word.ToString()) //获得当前的字符串
	word_tag := word.tag
	assert.Equal(t, "ID", word_tag.ToString()) //将他的tag用字符串显示
}

func TestWords(t *testing.T) {
	keyWords := GetKeyWords() //拿到所有的关键字
	orWord := keyWords[0]
	assert.Equal(t, "||", orWord.ToString())
	eqWord := keyWords[1]
	assert.Equal(t, "==", eqWord.ToString())
}
