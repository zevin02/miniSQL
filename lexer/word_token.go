package lexer

//Word 对sql的关键字进行管理,如果解析的时候，遇到这些字符串的话，应该被当作关键字，而不是变量字符串
//比如 int abc=123,lexeme 就是“abc”，而tag就是ID,
type Word struct {
	lexeme string //用户定义的字符串
	tag    *Token //当前关键字对应的token信息
}

func NewWordToken(s string, tag Tag) *Word {
	return &Word{
		lexeme: s,
		tag:    NewToken(tag),
	}
}

//ToString 返回当前的关键字字符串,对应的用户使用的字符串,变量就返回他对应的变量名，操作符，就返回他对应的操作符字符串
func (w *Word) ToString() string {
	return w.lexeme
}

//GetKeyWords 把所有能够形成字符串的关键字都抽取出来
func GetKeyWords() []*Word {
	//将这些变量设置成为关键字
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
	key_words = append(key_words, NewWordToken("WHERE", WHERE))
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
