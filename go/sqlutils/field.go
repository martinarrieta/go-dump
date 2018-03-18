package sqlutils

import (
	"database/sql"
	"encoding/json"
	"errors"

	_ "github.com/go-sql-driver/mysql"
)

type NullFieldData struct {
	RawBytes []byte
	value    interface{}
	Valid    bool // Valid is true if Bool is not NULL
	Column   *Column
}

func (this *NullFieldData) Scan(value interface{}) error {
	if value == nil && this.Column.IsNullable == true {
		this.Valid = false
		return errors.New("We got an error with the value ")
	}

	this.Valid = true
	this.value = value
	return nil
}

func (this *NullFieldData) Value() interface{} {

	switch this.Column.DataType {
	case "varchar", "char":
		return this.AsString()
	case "int", "bigint", "tinyint":
		return this.AsInt64()
	case "float":
		return this.AsFloat64()
	}

	return this.Value
}

func (this *NullFieldData) MarshalJSON() []byte {
	var ret []byte

	if this.Valid {
		ret, _ = json.Marshal(this.Value())
	} else {
		ret, _ = json.Marshal(nil)
	}
	return ret
}

func (this *NullFieldData) AsString() string {
	t := new(sql.NullString)
	t.Scan(this.value)
	return t.String
}

func (this *NullFieldData) AsInt64() int64 {
	t := new(sql.NullInt64)
	t.Scan(this.value)
	return t.Int64
}

func (this *NullFieldData) AsFloat64() float64 {
	t := new(sql.NullFloat64)
	t.Scan(this.value)
	return t.Float64
}
