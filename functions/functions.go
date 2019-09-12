package functions

import (
	dataframe2 "dataframe"
	"fmt"
	"math"
	"sort"
)

func Range(n int) []int {
	r := make([]int, n)
	for i := range r {
		r[i] = i
	}
	return r
}

func Copy(df dataframe2.DataFrame) dataframe2.DataFrame {
	return Select(df, Range(df.NCol()))
}

func Select(df dataframe2.DataFrame, indexes []int) dataframe2.DataFrame {
	ndf := dataframe2.NewDataFrame(df.NRow())

	series := df.GetAllSeries()

	for _, index := range indexes {
		ndf.SetSeries(series[index])
	}

	return ndf
}

func Combine(a, b dataframe2.DataFrame) (dataframe2.DataFrame, error) {
	if a.NRow() != b.NRow() {
		return nil, fmt.Errorf("nrow not equal")
	}

	c := dataframe2.NewDataFrame(a.NRow())
	for _, s := range a.GetAllSeries() {
		c.SetSeries(s)
	}

	for _, s := range b.GetAllSeries() {
		c.SetSeries(s)
	}

	return c, nil
}

func Sort(df dataframe2.DataFrame, colName string, lesser func(col dataframe2.Series) func(i, j int) bool) (dataframe2.DataFrame, error) {
	_, col, ok := df.GetSeries(colName)
	if !ok {
		return nil, fmt.Errorf("series not found")
	}

	rowIndexs := Range(df.NRow())
	sort.Slice(rowIndexs, lesser(col))

	ndf := dataframe2.NewDataFrame(df.NRow())
	for _, s := range df.GetAllSeries() {
		df.SetSeries(s.Select(rowIndexs))
	}
	return ndf, nil
}

func Max(s dataframe2.Series) (float64, error) {
	if s.Type() != dataframe2.Number {
		return 0, fmt.Errorf("not number series")
	}

	max := 0.0
	for i := 0; i < s.Len(); i++ {
		max = math.Max(max, s.Number(i))
	}
	return max, nil
}

func Min(s dataframe2.Series) (float64, error) {
	if s.Type() != dataframe2.Number {
		return 0, fmt.Errorf("not number series")
	}

	min := 0.0
	for i := 0; i < s.Len(); i++ {
		min = math.Min(min, s.Number(i))
	}
	return min, nil
}
