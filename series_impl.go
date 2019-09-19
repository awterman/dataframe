package dataframe

import (
	"fmt"
	"math"
	"strings"
)

type boolSeries struct {
	disableNumber
	disableString

	nameImpl
	hasValueImpl
	value []bool
}

func NewBoolSeries(name string, nrow int) Series {
	return &boolSeries{
		nameImpl:     nameImpl(name),
		hasValueImpl: make(hasValueImpl, nrow),
		value:        make([]bool, nrow),
	}
}

func (bs *boolSeries) Copy() Series {
	vcp := make([]bool, len(bs.value))
	copy(vcp, bs.value)

	nacp := make(hasValueImpl, len(bs.hasValueImpl))
	copy(nacp, bs.hasValueImpl)

	return &boolSeries{
		nameImpl:     bs.nameImpl,
		value:        vcp,
		hasValueImpl: nacp,
	}
}

func (bs *boolSeries) Type() Type         { return Bool }
func (bs *boolSeries) GetBool(i int) bool { return bs.value[i] }
func (bs *boolSeries) SetBool(i int, v bool) {
	bs.hasValueImpl[i] = true
	bs.value[i] = v
}

func (bs *boolSeries) SetNA(i int) {
	bs.hasValueImpl[i] = false
	bs.value[i] = false
}

func (bs *boolSeries) Select(indexes []int) Series {
	v := make([]bool, len(indexes))
	na := make(hasValueImpl, len(indexes))

	for i, index := range indexes {
		v[i] = bs.value[index]
		na[i] = bs.hasValueImpl[index]
	}
	return &boolSeries{
		nameImpl:     bs.nameImpl,
		value:        v,
		hasValueImpl: na,
	}
}

func (bs *boolSeries) String() string {
	builder := strings.Builder{}
	for i := range bs.value {
		if i != 0 {
			builder.WriteString(", ")
		}

		if bs.IsNA(i) {
			builder.WriteString("NA")
		} else {
			builder.WriteString(fmt.Sprint(bs.value[i]))
		}
	}
	return builder.String()
}

type numberSeries struct {
	disableBool
	disableString

	nameImpl
	hasValueImpl
	value []float64
}

func NewNumberSeries(name string, nrow int) Series {
	return &numberSeries{
		nameImpl:     nameImpl(name),
		hasValueImpl: make(hasValueImpl, nrow),
		value:        make([]float64, nrow),
	}
}

func (fs *numberSeries) Copy() Series {
	vcp := make([]float64, len(fs.value))
	copy(vcp, fs.value)

	nacp := make(hasValueImpl, len(fs.hasValueImpl))
	copy(nacp, fs.hasValueImpl)

	return &numberSeries{
		nameImpl:     fs.nameImpl,
		value:        vcp,
		hasValueImpl: nacp,
	}
}

func (fs *numberSeries) Type() Type              { return Number }
func (fs *numberSeries) GetNumber(i int) float64 { return fs.value[i] }
func (fs *numberSeries) SetNumber(i int, v float64) {
	fs.hasValueImpl[i] = true
	fs.value[i] = v
}

func (fs *numberSeries) SetNA(i int) {
	fs.hasValueImpl[i] = false
	fs.value[i] = math.NaN()
}

func (fs *numberSeries) Select(indexes []int) Series {
	v := make([]float64, len(indexes))
	na := make(hasValueImpl, len(indexes))

	for i, index := range indexes {
		v[i] = fs.value[index]
		na[i] = fs.hasValueImpl[index]
	}
	return &numberSeries{
		nameImpl:     fs.nameImpl,
		value:        v,
		hasValueImpl: na,
	}
}

func (fs *numberSeries) String() string {
	builder := strings.Builder{}
	for i := range fs.value {
		if i != 0 {
			builder.WriteString(", ")
		}

		if fs.IsNA(i) {
			builder.WriteString("NA")
		} else {
			builder.WriteString(fmt.Sprint(fs.value[i]))
		}
	}
	return builder.String()
}

type stringSeries struct {
	disableBool
	disableNumber

	nameImpl
	hasValueImpl
	value []string
}

func NewStringSeries(name string, nrow int) Series {
	return &stringSeries{
		nameImpl:     nameImpl(name),
		hasValueImpl: make(hasValueImpl, nrow),
		value:        make([]string, nrow),
	}
}

func (ss *stringSeries) Copy() Series {
	vcp := make([]string, len(ss.value))
	copy(vcp, ss.value)

	nacp := make(hasValueImpl, len(ss.hasValueImpl))
	copy(nacp, ss.hasValueImpl)

	return &stringSeries{
		nameImpl:     ss.nameImpl,
		value:        vcp,
		hasValueImpl: nacp,
	}
}

func (ss *stringSeries) Type() Type             { return String }
func (ss *stringSeries) GetString(i int) string { return ss.value[i] }
func (ss *stringSeries) SetString(i int, v string) {
	ss.hasValueImpl[i] = true
	ss.value[i] = v
}

func (ss *stringSeries) SetNA(i int) {
	ss.hasValueImpl[i] = false
	ss.value[i] = ""
}

func (ss *stringSeries) Select(indexes []int) Series {
	v := make([]string, len(indexes))
	na := make(hasValueImpl, len(indexes))

	for i, index := range indexes {
		v[i] = ss.value[index]
		na[i] = ss.hasValueImpl[index]
	}
	return &stringSeries{
		nameImpl:     ss.nameImpl,
		value:        v,
		hasValueImpl: na,
	}
}

func (ss *stringSeries) String() string {
	builder := strings.Builder{}
	for i := range ss.value {
		if i != 0 {
			builder.WriteString(", ")
		}

		if ss.IsNA(i) {
			builder.WriteString("NA")
		} else {
			builder.WriteString(fmt.Sprint(ss.value[i]))
		}
	}
	return builder.String()
}

type placeholderSeries struct {
	disableBool
	disableNumber
	disableString

	nameImpl
	nrow int
}

func NewPlaceholderSeries(name string, nrow int) Series {
	return &placeholderSeries{
		nameImpl: nameImpl(name),
		nrow:     nrow,
	}
}

func (ps *placeholderSeries) IsNA(int) bool       { return true }
func (ps *placeholderSeries) SetNA(int)           {}
func (ps *placeholderSeries) Len() int            { return ps.nrow }
func (ps *placeholderSeries) Type() Type          { return None }
func (ps *placeholderSeries) Copy() Series        { return ps }
func (ps *placeholderSeries) Select([]int) Series { return ps }
func (ps *placeholderSeries) String() string {
	s := strings.Repeat("NA ", ps.Len())
	return s[:len(s)-1]
}
