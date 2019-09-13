package dataframe

import "fmt"

type DataFrame interface {
	NRow() int
	NCol() int

	GetAllSeries() []Series
	GetSeries(name string) (index int, s Series, ok bool)
	SetSeries(series Series) error
	// SetSeriesDirectly set series by index, without lookup by name of series.
	SetSeriesDirectly(index int, series Series) error

	Select(indexes []int) DataFrame
	Copy() DataFrame
}

func NewDataFrame(nrow int) DataFrame {
	return &dataFrame{
		nrow: nrow,
	}
}

type dataFrame struct {
	nrow   int
	series []Series
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

func (df *dataFrame) Copy() DataFrame {
	return df.Select(range_(df.NCol()))
}

func (df *dataFrame) Select(indexes []int) DataFrame {
	ndf := NewDataFrame(df.NRow())

	series := df.GetAllSeries()

	for _, index := range indexes {
		ndf.SetSeries(series[index])
	}

	return ndf
}

func range_(n int) []int {
	r := make([]int, n)
	for i := range r {
		r[i] = i
	}
	return r
}
