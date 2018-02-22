package sqlutils

import (
  "fmt"
  _ "github.com/go-sql-driver/mysql"
  "reflect"
  "encoding/json"
)


type NullFieldData struct {
  	RawBytes  []byte
    value     interface{}
  	Valid     bool // Valid is true if Bool is not NULL
    column    *Column
  }

func (this *NullFieldData) Scan(value interface{}) error {
	if value == nil {
		this.Valid = false
		return nil
	}
	this.Valid = true
  this.value = value
  return nil
}

func (this *NullFieldData) Value() interface{} {
  switch this.column.DataType {
  case "varchar","char":
    return this.AsString()
  case "int","bigint","tinyint":
    return this.AsInt64()
  case "float":
    return this.AsFloat64()
  }
  return this.Value
}


func (this *NullFieldData) MarshalJSON() ([]byte) {
  var ret []byte

  if this.Valid {
		ret, _ = json.Marshal(this.Value())
	} else {
		ret, _ = json.Marshal(nil)
	}
  return ret
}


func (this *NullFieldData) AsRawBytes() []byte {
  var s []byte
  convertAssign(&s, this.value)
  return s
}

func (this *NullFieldData) AsString() string {
  if reflect.TypeOf(this.value).Kind() != reflect.String {
    var s string
    convertAssign(&s, this.value)
    return s
  }
  return this.value.(string)
}

func (this *NullFieldData) AsInt64() int64 {
  if reflect.TypeOf(this.value).Kind() != reflect.Int64 {
    var s int64
    convertAssign(&s, this.value)
    return s
  }
  return this.value.(int64)
}

func (this *NullFieldData) AsFloat64() float64 {
  if reflect.TypeOf(this.value).Kind() != reflect.Float64 {
    var s float64
    convertAssign(&s, this.value)
    return s
  }
  return this.value.(float64)
}

type Column struct {
  Name                  string
  OrdinalPosition       int
  Default               *string
  IsNullable            bool
  DataType              string
  CharacterMaxLength    *int
  CharacterOctetLength  *int
  NumericPrecision      *int
  NumericScale          *int
  DateTimePrecision     *int
  CharacterSetName      *string
  CollationName         *string
  ColumnKey             *string
  Extra                 *string
  ScanType              reflect.Type
}


type ColumnsMap map[string]int

type Table struct {
  Name            string
  Schema          string
  PrimaryKey      *Column
  Columns         []*Column
	ColumnsOrdinals ColumnsMap
}


func (this *Table) AddColumn( column *Column) {
  this.Columns = append(this.Columns, column)
}

//func (this *Table) GetColumn( columnName string) {
//  this.Columns = append(this.Columns, column)
//}

func (this *Table) GetPrimaryKeyName() string {
  return this.PrimaryKey.Name
}

func (this *Table) GetName() string {
  return this.Name
}

func (this *Table) GetSchema() string {
  return this.Schema
}

func (this *Table) GetSchameAndTable() string {
  return fmt.Sprintf("%s.%s", this.Schema, this.Name)
}
