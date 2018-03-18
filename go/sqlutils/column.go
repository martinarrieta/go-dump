package sqlutils

import "database/sql"

type Column struct {
	Name                 string
	OrdinalPosition      int
	Default              *string
	IsNullable           bool
	DataType             string
	CharacterMaxLength   *int
	CharacterOctetLength *int
	NumericPrecision     *int
	NumericScale         *int
	DateTimePrecision    *int
	CharacterSetName     *string
	CollationName        *string
	ColumnKey            string
	Extra                *string
	IsPrimaryKey         bool
}

func ProcessColumn(rows sql.Rows) (*Column, error) {
	column := new(Column)

	temp := new(string)
	err := rows.Scan(&column.Name, &column.OrdinalPosition, &column.Default, &temp,
		&column.DataType, &column.CharacterMaxLength, &column.CharacterOctetLength,
		&column.NumericPrecision, &column.NumericScale, &column.DateTimePrecision,
		&column.CharacterSetName, &column.CollationName, &column.ColumnKey,
		&column.Extra)

	if err != nil {
		return nil, err
	}

	if *temp == "YES" {
		column.IsNullable = true
	} else {
		column.IsNullable = false
	}

	if column.ColumnKey == "PRI" {
		column.IsPrimaryKey = true
	} else {
		column.IsPrimaryKey = false
	}

	return column, nil
}
