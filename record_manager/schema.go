package record_manager

type FIELD_TYPE int

const (
	INTEGER FIELD_TYPE = iota //整形类型
	VARCHAR                   //字读串的可变长度,最大不能超过65535
	BLOB                      //二进制类型

)

//FieldInfo 某个字段的类型
type FieldInfo struct {
	fieldType FIELD_TYPE //该字段的类型
	length    int        //该字段占用的长度
}

//newFieldInfo
func newFieldInfo(fieldType FIELD_TYPE, length int) *FieldInfo {
	return &FieldInfo{
		fieldType: fieldType,
		length:    length,
	}
}

//Schema 一个表中的一条记录包含哪些字段
type Schema struct {
	fields []string              //一个表中有多个字段,todo 有相同名字的字段
	info   map[string]*FieldInfo //返回某个字段的内容
}

func NewSchema() *Schema {
	return &Schema{
		fields: make([]string, 0),
		info:   make(map[string]*FieldInfo),
	}
}

//AddField create table e (age int,name string),这个sql语句实际上就是调用schema的添加字段的接口
//添加任意一个类型的字段
func (s *Schema) AddField(fieldName string, fieldType FIELD_TYPE, length int) {
	s.fields = append(s.fields, fieldName)
	s.info[fieldName] = newFieldInfo(fieldType, length)
}

//AddIntField 添加一个int类型的字段
func (s *Schema) AddIntField(fieldName string) {
	//对于整形字段来说，长度没有用，都是固定的大小
	s.AddField(fieldName, INTEGER, 0)
}

// AddStringField 添加一个string类型的字段
func (s *Schema) AddStringField(fileName string, length int) {
	s.AddField(fileName, VARCHAR, length)
}

//Add 整形类型或字符串类型都能添加
func (s *Schema) Add(fieldName string, sch SchemaInterface) {
	filedType := sch.Type(fieldName)         //获得fieldName在当前的表中的类型
	length := sch.Length(fieldName)          //获得该字段对应的长度
	s.AddField(fieldName, filedType, length) //将当前的字段添加进去
}

//AddAll 把该对象的所有描述都加入进来
func (s *Schema) AddAll(sch SchemaInterface) {
	//将给定的sch的所有对象全部添加到当前的表中哦
	fields := sch.Fields() //获得当前表的所有表字段
	for _, field := range fields {
		s.Add(field, sch) //将sch中的每个字段都增加到当前的s中
	}
}

//Fields 返回所有字段的名称
func (s *Schema) Fields() []string {
	return s.fields
}

//HashField 判断某个表中是否有某个字段
func (s *Schema) HashField(fieldName string) bool {
	for _, field := range s.fields {
		if field == fieldName {
			return true
		}
	}
	return false
}

//Type 返回某个字段对应的类型
func (s *Schema) Type(fieldName string) FIELD_TYPE {
	return s.info[fieldName].fieldType //从字段的info中返回他的类型
}

//Length 返回该字段对应列的长度
func (s *Schema) Length(fieldName string) int {
	return s.info[fieldName].length

}
