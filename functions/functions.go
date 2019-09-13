package functions

import (
	"fmt"
	"math"
	"sort"

	"github.com/awterman/dataframe"
)

func range_(n int) []int {
	r := make([]int, n)
	for i := range r {
		r[i] = i
	}
	return r
}

func Combine(a, b dataframe.DataFrame) (dataframe.DataFrame, error) {
	if a.NRow() != b.NRow() {
		return nil, fmt.Errorf("nrow not equal")
	}

	c := dataframe.NewDataFrame(a.NRow())
	for _, s := range a.GetAllSeries() {
		c.AppendSeries(s)
	}

	for _, s := range b.GetAllSeries() {
		c.AppendSeries(s)
	}

	return c, nil
}

func Sort(df dataframe.DataFrame, colName string, lesser func(col dataframe.Series) func(i, j int) bool) (dataframe.DataFrame, error) {
	_, col, ok := df.GetSeries(colName)
	if !ok {
		return nil, fmt.Errorf("series not found")
	}

	rowIndexes := range_(df.NRow())
	sort.Slice(rowIndexes, lesser(col))

	ndf := dataframe.NewDataFrame(df.NRow())
	for _, s := range df.GetAllSeries() {
		df.AppendSeries(s.Select(rowIndexes))
	}
	return ndf, nil
}

func Max(s dataframe.Series) (float64, error) {
	if s.Type() != dataframe.Number {
		return 0, fmt.Errorf("not number series")
	}

	max := 0.0
	for i := 0; i < s.Len(); i++ {
		max = math.Max(max, s.Number(i))
	}
	return max, nil
}

func Min(s dataframe.Series) (float64, error) {
	if s.Type() != dataframe.Number {
		return 0, fmt.Errorf("not number series")
	}

	min := 0.0
	for i := 0; i < s.Len(); i++ {
		min = math.Min(min, s.Number(i))
	}
	return min, nil
}
