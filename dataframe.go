package dataframe

import "fmt"

type DataFrame interface {
	NRow() int
	NCol() int

	GetAllSeries() []Series
	GetSeries(name string) (index int, s Series, ok bool)
	SetSeries(series Series) error
	// SetSeriesDirectly set series by index, without looking up by name of series.
	SetSeriesDirectly(index int, series Series) error
	AppendSeries(series ...Series) error

	Copy() DataFrame
	Select(indexes []int) DataFrame

	String() string
}

type formatter func(DataFrame) string

func NewDataFrame(nrow int, options ...Option) DataFrame {
	ov := newOptionValues()
	for _, o := range options {
		o(ov)
	}

	return &dataFrame{
		nrow: nrow,

		formatter: ov.formatter,
	}
}

type dataFrame struct {
	nrow   int
	series []Series

	formatter formatter
}

func (df *dataFrame) NRow() int {
	return df.nrow
}

func (df *dataFrame) NCol() int {
	return len(df.series)
}

func (df *dataFrame) GetAllSeries() []Series {
	return df.series
}

func (df *dataFrame) GetSeries(name string) (index int, s Series, ok bool) {
	for i, s := range df.series {
		if s.Name() == name {
			return i, s, true
		}
	}
	return -1, nil, false
}

func (df *dataFrame) SetSeries(series Series) error {
	if series.Len() != df.nrow {
		return fmt.Errorf("nrow not equal")
	}

	if index, _, ok := df.GetSeries(series.Name()); ok {
		df.series[index] = series
		return nil
	}

	df.series = append(df.series, series)
	return nil
}

func (df *dataFrame) SetSeriesDirectly(index int, series Series) error {
	if index > len(df.series) {
		return fmt.Errorf("out of range")
	}

	if series.Len() != df.nrow {
		return fmt.Errorf("nrow not equal")
	}

	if index == len(df.series) {
		df.series = append(df.series, series)
		return nil
	}

	df.series[index] = series
	return nil
}

func (df *dataFrame) AppendSeries(series ...Series) error {
	for _, s := range series {
		if s.Len() != df.nrow {
			return fmt.Errorf("%s: nrow not equal", s.Name())
		}
	}

	df.series = append(df.series, series...)
	return nil
}

func (df *dataFrame) Copy() DataFrame {
	ndf := NewDataFrame(df.NRow())

	for _, s := range ndf.GetAllSeries() {
		ndf.AppendSeries(s.Copy())
	}

	return ndf
}

func (df *dataFrame) Select(indexes []int) DataFrame {
	ndf := NewDataFrame(df.NRow())

	series := df.GetAllSeries()

	for _, index := range indexes {
		ndf.AppendSeries(series[index])
	}

	return ndf
}

func (df *dataFrame) String() string {
	return df.formatter(df)
}
