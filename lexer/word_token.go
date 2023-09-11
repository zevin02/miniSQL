package lexer

//对sql的关键字进行管理,如果解析的时候，遇到这些字符串的话，应该被当作关键字，而不是变量
type Word struct {
	lexeme string //关键字
	tag    *Token //当前关键字对应的token信息
}

func NewWordToken(s string, tag Tag) *Word {
	return &Word{
		lexeme: s,
		tag:    NewToken(tag),
	}
}

//ToString 返回当前的关键字字符串
func (w *Word) ToString() string {
	return w.lexeme
}

func GetKeyWords() []*Word {
	//这些字符串都不会被解析成变量
	key_words := []*Word{}
	key_words = append(key_words, NewWordToken("||", OR))
	key_words = append(key_words, NewWordToken("==", EQ))
	key_words = append(key_words, NewWordToken("!=", NE))
	key_words = append(key_words, NewWordToken("<=", LE))
	key_words = append(key_words, NewWordToken(">=", GE))
	//增加SQL语言对应关键字
	key_words = append(key_words, NewWordToken("AND", AND))
	key_words = append(key_words, NewWordToken("SELECT", SELECT))
	key_words = append(key_words, NewWordToken("FROM", FROM))
	key_words = append(key_words, NewWordToken("INSERT", INSERT))
	key_words = append(key_words, NewWordToken("INTO", INTO))
	key_words = append(key_words, NewWordToken("VALUES", VALUES))
	key_words = append(key_words, NewWordToken("DELETE", DELETE))
	key_words = append(key_words, NewWordToken("UPDATE", UPDATE))
	key_words = append(key_words, NewWordToken("SET", SET))
	key_words = append(key_words, NewWordToken("CREATE", CREATE))
	key_words = append(key_words, NewWordToken("TABLE", TABLE))
	key_words = append(key_words, NewWordToken("INT", INT))
	key_words = append(key_words, NewWordToken("VARCHAR", VARCHAR))
	key_words = append(key_words, NewWordToken("VIEW", VIEW))
	key_words = append(key_words, NewWordToken("AS", AS))
	key_words = append(key_words, NewWordToken("INDEX", INDEX))
	key_words = append(key_words, NewWordToken("ON", ON))
	return key_words
}
