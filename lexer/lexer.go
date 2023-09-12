package lexer

import (
	"bufio"
	"strconv"
	"strings"
	"unicode"
)

//词法解析器
//将源代码的字符串读入，并把字符串规定到相应的分类中，让token来标志对应的字符串，我们需要一个字符一个字符的读取

type Lexer struct {
	lexeme string //保存用户此时输入的字符串
	//用于存储已经识别的词法单元，在构建语法树的时候就能用上
	lexemeStack []string //存储已识别出来的标记的文本内容
	tokenStack  []*Token //已经识别出来标记的标记集合
	readPointer int
	peek        byte              //读入的字符
	line        int               //当前的字符串处在第几行
	reader      *bufio.Reader     //提供了缓冲读取的功能，用来读取字节流
	keyWords    map[string]*Token //把字符串按照关键字来存储，字符串对应到特定的关键字token
}

//NewLexer source给定的代码的字符串
func NewLexer(source string) *Lexer {
	str := strings.NewReader(source)                      //根据给定的字符串构造reader对象,允许从字符串中进行逐个字符的读取
	sourceReader := bufio.NewReaderSize(str, len(source)) //基于str的Reader，设置缓冲区的长度为源字符串的长度，提高读取效率
	lexer := &Lexer{
		line:     1,
		reader:   sourceReader,
		keyWords: make(map[string]*Token),
	}
	lexer.reserve() //保留原先预订关键字
	return lexer
}

//reserve 先保留所有的已经设定好了的关键字，方便之后在词法分析中快速的识别他们
func (l *Lexer) reserve() {
	keyWords := GetKeyWords() //先获得所有的关键字
	for _, keyWord := range keyWords {
		l.keyWords[keyWord.ToString()] = keyWord.tag //将用户的字符串归类到对应的token
	}
}

//Readch 从代码中读取一个一个的字符
func (l *Lexer) Readch() error {
	char, err := l.reader.ReadByte() //读取一个字符
	l.peek = char                    //把读取的字符进行暂存
	return err
}

//ReadCharacter 判断当前读取的字符和给定的字符是否相同，如果不相同就返回false,如果相同就把当前读到这个设置成' ',并返回true
func (l *Lexer) ReadCharacter(c byte) (bool, error) {
	chars, err := l.reader.Peek(1)
	if err != nil {
		return false, err
	}
	peekChar := chars[0]
	if peekChar != c {
		//读入的字符和输入的字符不一样
		return false, nil
	}
	//如果当前读入的字符和指定的字符一样，就设置成‘ ’
	l.Readch() //越过当前peek字符
	return true, nil
}

//Scan 不断的扫描原sql代码，进行词法分析，分类，生成相对应的token
func (l *Lexer) Scan() (*Token, error) {
	if l.readPointer < len(l.lexemeStack) {
		//检查是否有保存的token没有被使用，将从已经保存的标记中获取一个标记并返回
		//这个通常是实现预读的功能
		l.lexeme = l.lexemeStack[l.readPointer]
		token := l.tokenStack[l.readPointer]
		l.readPointer += 1
		return token, nil
	} else {
		l.readPointer += 1
	}

	for {
		//扫描一行的代码
		err := l.Readch()
		if err != nil {
			return NewToken(ERROR), err
		}
		if l.peek == ' ' || l.peek == '\t' {
			continue
		} else if l.peek == '\n' {
			//换行符的话，行号就需要增加
			l.line = l.line + 1
		} else {
			//当前是一个正常的字符,
			break
		}
	}
	l.lexeme = ""
	switch l.peek { //查看是否是特殊字符
	case ',':
		l.lexeme = ","
		l.lexemeStack = append(l.lexemeStack, l.lexeme) //将当前的l.lexeme添加到stack中
		token := NewToken(COMMA)
		l.tokenStack = append(l.tokenStack, token)
		return token, nil
	case '{':
		l.lexeme = "{"
		l.lexemeStack = append(l.lexemeStack, l.lexeme) //将当前的l.lexeme添加到stack中
		token := NewToken(LEFT_BRACE)
		l.tokenStack = append(l.tokenStack, token)
		return token, nil
	case '}':
		l.lexeme = "}"
		l.lexemeStack = append(l.lexemeStack, l.lexeme) //将当前的l.lexeme添加到stack中
		token := NewToken(RIGHT_BRACE)
		l.tokenStack = append(l.tokenStack, token)
		return token, nil

	case '(':
		l.lexeme = "("
		l.lexemeStack = append(l.lexemeStack, l.lexeme) //将当前的l.lexeme添加到stack中
		token := NewToken(LEFT_BRACKET)
		l.tokenStack = append(l.tokenStack, token)
		return token, nil
	case ')':
		l.lexeme = ")"
		l.lexemeStack = append(l.lexemeStack, l.lexeme) //将当前的l.lexeme添加到stack中
		token := NewToken(RIGHT_BRACKET)
		l.tokenStack = append(l.tokenStack, token)
		return token, nil
	case '+':
		l.lexeme = "+"
		l.lexemeStack = append(l.lexemeStack, l.lexeme) //将当前的l.lexeme添加到stack中
		token := NewToken(PLUS)
		l.tokenStack = append(l.tokenStack, token)
		return token, nil
	case '-':
		l.lexeme = "-"
		l.lexemeStack = append(l.lexemeStack, l.lexeme) //将当前的l.lexeme添加到stack中
		token := NewToken(MINUS)
		l.tokenStack = append(l.tokenStack, token)
		return token, nil
	case '&':
		l.lexeme = "&"
		//如果当前是&,需要检查当前是否是&&,所以需要往后面多读取一位
		if ok, err := l.ReadCharacter('&'); ok {
			l.lexeme = "&&"
			word := NewWordToken("&&", AND)
			l.lexemeStack = append(l.lexemeStack, l.lexeme)
			l.tokenStack = append(l.tokenStack, word.tag)
			if err != nil {
				return NewToken(ERROR), err
			}
			//后面的一个仍然是&,就符合条件
			return word.tag, nil
		} else {
			//否则就是一个与操作符
			l.lexemeStack = append(l.lexemeStack, l.lexeme)
			token := NewToken(AND_OPERATOR)
			l.tokenStack = append(l.tokenStack, token)
			return token, nil
		}

	case '|':
		//如果当前是&,需要检查当前是否是&&,所以需要往后面多读取一位
		l.lexeme = "|"
		//如果当前是&,需要检查当前是否是&&,所以需要往后面多读取一位
		if ok, err := l.ReadCharacter('|'); ok {
			l.lexeme = "||"
			word := NewWordToken("||", OR)
			l.lexemeStack = append(l.lexemeStack, l.lexeme)
			l.tokenStack = append(l.tokenStack, word.tag)
			if err != nil {
				return NewToken(ERROR), err
			}
			//后面的一个仍然是&,就符合条件
			return word.tag, nil
		} else {
			//否则就是一个与操作符
			l.lexemeStack = append(l.lexemeStack, l.lexeme)
			token := NewToken(OR_OPERATOR)
			l.tokenStack = append(l.tokenStack, token)
			return token, nil
		}
	case '=':
		//如果当前是&,需要检查当前是否是&&,所以需要往后面多读取一位
		l.lexeme = "="
		//如果当前是&,需要检查当前是否是&&,所以需要往后面多读取一位
		if ok, err := l.ReadCharacter('='); ok {
			l.lexeme = "=="
			word := NewWordToken("==", EQ)
			l.lexemeStack = append(l.lexemeStack, l.lexeme)
			l.tokenStack = append(l.tokenStack, word.tag)
			if err != nil {
				return NewToken(ERROR), err
			}
			//后面的一个仍然是&,就符合条件
			return word.tag, nil
		} else {
			//否则就是一个与操作符
			l.lexemeStack = append(l.lexemeStack, l.lexeme)
			token := NewToken(ASSIGN_OPERATOR)
			l.tokenStack = append(l.tokenStack, token)
			return token, nil
		}
	case '!':
		//如果当前是&,需要检查当前是否是&&,所以需要往后面多读取一位
		l.lexeme = "!"
		//如果当前是&,需要检查当前是否是&&,所以需要往后面多读取一位
		if ok, err := l.ReadCharacter('='); ok {
			l.lexeme = "!="
			word := NewWordToken("!=", NE)
			l.lexemeStack = append(l.lexemeStack, l.lexeme)
			l.tokenStack = append(l.tokenStack, word.tag)
			if err != nil {
				return NewToken(ERROR), err
			}
			//后面的一个仍然是&,就符合条件
			return word.tag, nil
		} else {
			//否则就是一个与操作符
			l.lexemeStack = append(l.lexemeStack, l.lexeme)
			token := NewToken(NEGATE_OPERATOR)
			l.tokenStack = append(l.tokenStack, token)
			return token, nil
		}
	case '<':
		//如果当前是&,需要检查当前是否是&&,所以需要往后面多读取一位
		l.lexeme = "<"
		//如果当前是&,需要检查当前是否是&&,所以需要往后面多读取一位
		if ok, err := l.ReadCharacter('='); ok {
			l.lexeme = "<="
			word := NewWordToken("<=", LE)
			l.lexemeStack = append(l.lexemeStack, l.lexeme)
			l.tokenStack = append(l.tokenStack, word.tag)
			if err != nil {
				return NewToken(ERROR), err
			}
			//后面的一个仍然是&,就符合条件
			return word.tag, nil
		} else {
			//否则就是一个与操作符
			l.lexemeStack = append(l.lexemeStack, l.lexeme)
			token := NewToken(LESS_OPERATOR)
			l.tokenStack = append(l.tokenStack, token)
			return token, nil
		}

	case '>':
		//如果当前是&,需要检查当前是否是&&,所以需要往后面多读取一位
		l.lexeme = ">"
		//如果当前是&,需要检查当前是否是&&,所以需要往后面多读取一位
		if ok, err := l.ReadCharacter('='); ok {
			l.lexeme = ">="
			word := NewWordToken(">=", GE)
			l.lexemeStack = append(l.lexemeStack, l.lexeme)
			l.tokenStack = append(l.tokenStack, word.tag)
			if err != nil {
				return NewToken(ERROR), err
			}
			//后面的一个仍然是&,就符合条件
			return word.tag, nil
		} else {
			//否则就是一个与操作符
			l.lexemeStack = append(l.lexemeStack, l.lexeme)
			token := NewToken(GREATER_OPERATOR)
			l.tokenStack = append(l.tokenStack, token)
			return token, nil
		}
	case '"':
		//对于这个开头的，会循环读取字符，直到读取到下一个“为止
		for {
			err := l.Readch()
			if l.peek == '"' {
				l.lexemeStack = append(l.lexemeStack, l.lexeme)
				token := NewToken(STRING)
				l.tokenStack = append(l.tokenStack, token)
				return token, nil
			}

			if err != nil {
				panic("string no end with quota")
			}
			l.lexeme += string(l.peek) //将读取到的字符串拼接起来
		}
	}
	//上面的情况都不是

	//判断读入的字符串是否是数字，如果是数字的话，就需要一直循环往后读，直到不是数字为止
	if unicode.IsNumber(rune(l.peek)) {
		//如果当前的字符是数字，那么就需要一直到没有数字为止
		var v int //当前的v来存储读取到的整形数
		var err error
		for {
			num, err := strconv.Atoi(string(l.peek)) //将当前的字符转化为数字
			if err != nil {
				//转换失败就说明读取结束了
				if l.peek != 0 {
					l.UnRead()
				}
				break
			}
			v = v*10 + num
			l.lexeme += string(l.peek)
			l.Readch() //读取下一个字符
		}
		if l.peek != '.' {
			//如果当前不是‘.'说明当前数字读取完了，当前数字不是小数，而是一个整数，就把当前整数返回
			l.lexemeStack = append(l.lexemeStack, l.lexeme)
			token := NewToken(NUM)
			token.lexeme = l.lexeme
			l.tokenStack = append(l.tokenStack, token)
			return token, err
		}
		l.lexeme += string(l.peek)
		l.Readch()
		//这个就说明他是一个小数
		x := float64(v) //把当前放到x
		d := float64(10)
		for {
			//继续往后读取数据
			l.Readch()
			num, err := strconv.Atoi(string(l.peek))
			if err != nil {
				if l.peek != 0 {
					l.UnRead()
				}
				break
			}
			//把小数点后面的字符进行转换
			x = x + float64(num)/d
			d = d * 10 //进制每次都要增加
			l.lexeme += string(l.peek)
		}
		l.lexemeStack = append(l.lexemeStack, l.lexeme)
		token := NewToken(REAL)
		token.lexeme = l.lexeme
		l.tokenStack = append(l.tokenStack, token)
		return token, nil
		//当前是一个小数，就返回REAL类型的token
	}
	//读取变量字符串，注意看读取到的字符时不是关键字
	if unicode.IsLetter(rune(l.peek)) {
		var buffer []byte //把字符放到缓冲区中
		for {
			buffer = append(buffer, l.peek)
			l.lexeme += string(l.peek)
			//继续往后读取一个字符
			l.Readch()
			if !unicode.IsLetter(rune(l.peek)) {
				//当前已经不是字符了，说明字符串已经读取完成了
				if l.peek != 0 {
					l.UnRead()
				}
				break
			}
		}
		s := string(buffer)
		token, ok := l.keyWords[s] //检查一下单前的字符串是不是一个关键字,如果是的话
		if ok {
			l.lexemeStack = append(l.lexemeStack, l.lexeme) //将当前的字符串保存起来
			l.tokenStack = append(l.tokenStack, token)      //将当前的字符串的token保存起来
			//当前是一个关键字,直接返回这个关键字对应的token即可
			return token, nil
		} else {
			l.lexemeStack = append(l.lexemeStack, l.lexeme)
			token = NewToken(ID)
			token.lexeme = l.lexeme
			l.tokenStack = append(l.tokenStack, token)
			return token, nil
		}
	}

	//读取到结束了,直接返回
	return NewToken(EOF), nil
}

func (l *Lexer) UnRead() error {
	return l.reader.UnreadByte()
}
