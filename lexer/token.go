package lexer

type Tag uint32

//123+456经过词法分析之后变成了NUM PLUS NUM,语法分析检验这个是否正确
//123+“abc”，词法解析之后变成了NUM PLUS STRING,编译器就会输出错误

//添加一些sql的关键字token最基础的sql的词
const (
	//这些都是一些关键字
	AND Tag = iota + 256
	OR
	EQ
	FALSE
	TRUE
	GE
	ID //identifier
	LE
	FLOAT
	MINUS //-
	PLUS  //+
	NE
	NUM           //对应数字
	REAL          //对应浮点数：3.14这样的
	LEFT_BRACE    // "{"
	RIGHT_BRACE   // "}"
	LEFT_BRACKET  //"("
	RIGHT_BRACKET //")"
	AND_OPERATOR
	OR_OPERATOR
	ASSIGN_OPERATOR
	NEGATE_OPERATOR
	LESS_OPERATOR
	GREATER_OPERATOR
	BASIC //对应int , float, bool, char 等类型定义
	STRING
	//新增SQL对应关键字
	SELECT
	FROM
	WHERE
	INSERT
	INTO
	VALUES
	DELETE
	UPDATE
	SET
	CREATE
	TABLE
	INT
	VARCHAR
	VIEW
	AS
	INDEX
	ON
	COMMA
	//SQL关键字定义结束
	EOF //文件的结束

	ERROR
)

var TokenMap = make(map[Tag]string) //把这些标号，就可以转化成相应的字符串

//词法解析，就是把字符串进行一个归类的过程
//这样就可以把每个关键字对应到他们相应的字符串上面，把所有的字符串进行归类，每个类别都由一个标号来标识，
func init() {
	//init函数会自动执行，如果有多个init函数，编译器会决定怎么执行
	//初始化SQL关键字对应字符串,将上面定义的值，和相应的字符串进行比对
	TokenMap[AND] = "AND"
	TokenMap[SELECT] = "SELECT"
	TokenMap[FROM] = "FROM"
	TokenMap[WHERE] = "WHERE"
	TokenMap[INSERT] = "INSERT"
	TokenMap[INTO] = "INTO"
	TokenMap[VALUES] = "VALUES"
	TokenMap[DELETE] = "DELETE"
	TokenMap[UPDATE] = "UPDATE"
	TokenMap[SET] = "SET"
	TokenMap[CREATE] = "CREATE"
	TokenMap[TABLE] = "TABLE"
	TokenMap[INT] = "INT"
	TokenMap[VARCHAR] = "VARCHAR"
	TokenMap[VIEW] = "VIEW"
	TokenMap[AS] = "AS"
	TokenMap[INDEX] = "INDEX"
	TokenMap[ON] = "ON"
	TokenMap[COMMA] = ","
	TokenMap[BASIC] = "BASIC"
	TokenMap[EQ] = "EQ"
	TokenMap[FALSE] = "FALSE"
	TokenMap[GE] = "GE"
	TokenMap[ID] = "ID"
	TokenMap[INT] = "int"
	TokenMap[FLOAT] = "float"

	TokenMap[LE] = "<="
	TokenMap[MINUS] = "-"
	TokenMap[PLUS] = "+"
	TokenMap[NE] = "!="
	TokenMap[NUM] = "NUM"
	TokenMap[OR] = "OR"
	TokenMap[REAL] = "REAL"
	TokenMap[TRUE] = "TRUE"
	//操作符
	TokenMap[AND_OPERATOR] = "&"
	TokenMap[OR_OPERATOR] = "|"
	TokenMap[ASSIGN_OPERATOR] = "="
	TokenMap[NEGATE_OPERATOR] = "!"
	TokenMap[LESS_OPERATOR] = "<"
	TokenMap[GREATER_OPERATOR] = ">"
	TokenMap[LEFT_BRACE] = "{"
	TokenMap[RIGHT_BRACE] = "}"
	TokenMap[LEFT_BRACKET] = "("
	TokenMap[RIGHT_BRACKET] = ")"
	TokenMap[EOF] = "EOF"
	TokenMap[ERROR] = "ERROR"

}

//Token 管理全局的一些token的使用,标号和对应标号的字符串形式
type Token struct {
	Tag    Tag
	lexeme string //标号的字符串形式：LE ->"<"
}

func NewToken(tag Tag) *Token {
	return &Token{
		lexeme: "",
		Tag:    tag,
	}
}

func NewTokenWithString(tag Tag, lexeme string) *Token {
	return &Token{
		lexeme: lexeme,
		Tag:    tag,
	}
}

//ToString 把这个标号对应的标号按照字符串的形式打印回去
func (t *Token) ToString() string {
	if t.lexeme == "" { //如果当前的lexeme没有使用，就直接调用token_map中的数据
		return TokenMap[t.Tag]
	}
	return t.lexeme
}
