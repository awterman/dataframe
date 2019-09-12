package dataframe

import "fmt"

type DataFrame interface {
	NRow() int
	NCol() int

	GetAllSeries() []Series
	GetSeries(name string) (index int, s Series, ok bool)
	SetSeries(series Series) error
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
	return 0, nil, false
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
