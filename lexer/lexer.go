package lexer

import (
	"bufio"
	"strconv"
	"strings"
	"unicode"
)

//将源代码的字符串读入，并把字符串规定到相应的分类中，让token来标志对应的字符串，我们需要一个字符一个字符的读取

type Lexer struct {
	peek     byte              //读入的字符
	line     int               //当前的字符串处在第几行
	reader   *bufio.Reader     //提供了缓冲读取的功能，用来读取字节流
	keyWords map[string]*Token //把字符串按照关键字来存储，字符串对应到特定的关键字token
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
		l.keyWords[keyWord.ToString()] = keyWord.tag //将用户的字符串归类到对应的token中
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
	err := l.Readch() //读入一个字符
	if err != nil {
		return false, err
	}
	if l.peek != c {
		//读入的字符和输入的字符不一样
		return false, nil
	}
	//如果当前读入的字符和指定的字符一样，就设置成‘ ’
	l.peek = ' '
	return true, nil
}

func (l *Lexer) Scan() (*Token, error) {
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
	switch l.peek { //查看是否是特殊字符
	case '{':
		return NewToken(LEFT_BRACE), nil
	case '}':
		return NewToken(RIGHT_BRACE), nil
	case '+':
		return NewToken(PLUS), nil
	case '-':
		return NewToken(MINUS), nil
	case '&':
		//如果当前是&,需要检查当前是否是&&,所以需要往后面多读取一位
		if ok, err := l.ReadCharacter('&'); ok {
			if err != nil {
				return NewToken(ERROR), err
			}
			//后面的一个仍然是&,就符合条件
			word := NewWordToken("&&", AND)
			return word.tag, nil
		} else {
			//否则就是一个与操作符
			return NewToken(AND_OPERATOR), nil
		}
	case '|':
		//如果当前是&,需要检查当前是否是&&,所以需要往后面多读取一位
		if ok, err := l.ReadCharacter('|'); ok {
			if err != nil {
				return NewToken(ERROR), err
			}
			//后面的一个仍然是&,就符合条件
			word := NewWordToken("||", OR)
			return word.tag, nil
		} else {
			//否则就是一个与操作符
			return NewToken(OR_OPERATOR), nil
		}
	case '=':
		//如果当前是&,需要检查当前是否是&&,所以需要往后面多读取一位
		if ok, err := l.ReadCharacter('='); ok {
			if err != nil {
				return NewToken(ERROR), err
			}
			//后面的一个仍然是&,就符合条件
			word := NewWordToken("==", EQ)
			return word.tag, nil
		} else {
			//否则就是一个与操作符
			return NewToken(ASSIGN_OPERATOR), nil
		}
	case '!':
		//如果当前是&,需要检查当前是否是&&,所以需要往后面多读取一位
		if ok, err := l.ReadCharacter('='); ok {
			if err != nil {
				return NewToken(ERROR), err
			}
			//后面的一个仍然是&,就符合条件
			word := NewWordToken("!=", NE)
			return word.tag, nil
		} else {
			//否则就是一个与操作符
			return NewToken(NEGATE_OPERATOR), nil
		}
	case '<':
		//如果当前是&,需要检查当前是否是&&,所以需要往后面多读取一位
		if ok, err := l.ReadCharacter('='); ok {
			if err != nil {
				return NewToken(ERROR), err
			}
			//后面的一个仍然是&,就符合条件
			word := NewWordToken("<=", LE)
			return word.tag, nil
		} else {
			//否则就是一个与操作符
			return NewToken(LESS_OPERATOR), nil
		}

	case '>':
		//如果当前是&,需要检查当前是否是&&,所以需要往后面多读取一位
		if ok, err := l.ReadCharacter('='); ok {
			if err != nil {
				return NewToken(ERROR), err
			}
			//后面的一个仍然是&,就符合条件
			word := NewWordToken(">=", GE)
			return word.tag, nil
		} else {
			//否则就是一个与操作符
			return NewToken(GREATER_OPERATOR), nil
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
				break
			}
			v = v*10 + num
			l.Readch() //读取下一个字符
		}
		if l.peek != '.' {
			//如果当前不是‘.'说明当前数字读取完了，当前数字不是小数，而是一个整数，就把当前整数返回
			return NewToken(NUM), err
		}
		//这个就说明他是一个小数
		x := float64(v) //把当前放到x
		d := float64(10)
		for {
			//继续往后读取数据
			l.Readch()
			num, err := strconv.Atoi(string(l.peek))
			if err != nil {
				break
			}
			//把小数点后面的字符进行转换
			x = x + float64(num)/d
			d = d * 10 //进制每次都要增加
		}
		//当前是一个小数，就返回REAL类型的token
		return NewToken(REAL), nil
	}
	//读取变量字符串，注意看读取到的字符时不是关键字
	if unicode.IsLetter(rune(l.peek)) {
		var buffer []byte //把字符放到缓冲区中
		for {
			buffer = append(buffer, l.peek)
			//继续往后读取一个字符
			l.Readch()
			if !unicode.IsLetter(rune(l.peek)) {
				//当前已经不是字符了，说明字符串已经读取完成了
				break
			}
		}
		s := string(buffer)
		token, ok := l.keyWords[s] //检查一下单前的字符串是不是一个关键字,如果是的话
		if ok {
			//当前是一个关键字,直接返回这个关键字对应的token即可
			return token, nil
		} else {
			//当前是一个字符串,就只是一个变量,identifier类型返回
			return NewToken(ID), nil
		}
	}

	//读取到结束了,直接返回
	return NewToken(EOF), nil
}
