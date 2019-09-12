package functions

import (
	"dataframe"
	"fmt"
)

type Builder struct {
	dataframe.DataFrame

	rowsWritten int
}

func NewBuilder(nrow int) *Builder {
	return &Builder{
		DataFrame: dataframe.NewDataFrame(nrow),
	}

}

func (b *Builder) parseValue(v interface{}) (t dataframe.Type, set func(dataframe.Series)) {
	if v == nil {
		return dataframe.None, func(dataframe.Series) {}
	}

	var n float64

	switch vv := v.(type) {
	case bool:
		return dataframe.Bool, func(s dataframe.Series) { s.SetBool(b.rowsWritten, vv) }
	case string:
		return dataframe.String, func(s dataframe.Series) { s.SetString(b.rowsWritten, vv) }
	case int:
		n = float64(vv)
	case int8:
		n = float64(vv)
	case int16:
		n = float64(vv)
	case int32:
		n = float64(vv)
	case int64:
		n = float64(vv)
	case uint:
		n = float64(vv)
	case uint8:
		n = float64(vv)
	case uint16:
		n = float64(vv)
	case uint32:
		n = float64(vv)
	case uint64:
		n = float64(vv)
	default:
		panic(fmt.Sprintf("unsupported type: %T", vv))
	}
	return dataframe.Number, func(s dataframe.Series) { s.SetNumber(b.rowsWritten, n) }
}

func (b *Builder) WriteRow(row map[string]interface{}) {
	for colName, value := range row {
		t, set := b.parseValue(value)

		_, col, ok := b.GetSeries(colName)
		if !ok || (col.Type() == dataframe.None && t != dataframe.None) {
			col := dataframe.NewSeries(t, colName, b.NRow())
			b.SetSeries(col)
		}

		set(col)
	}

	b.rowsWritten++
}

func (b *Builder) Build() dataframe.DataFrame {
	return b.DataFrame
}
