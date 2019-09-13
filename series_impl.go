package dataframe

type boolSeries struct {
	disableNumber
	disableString

	nameImpl
	naImpl
	bools []bool
}

func NewBoolSeries(name string, nrow int) Series {
	return &boolSeries{
		nameImpl: nameImpl(name),
		naImpl:   make(naImpl, nrow),
		bools:    make([]bool, nrow),
	}
}

func (bs *boolSeries) Copy() Series {
	vcp := make([]bool, len(bs.bools))
	copy(vcp, bs.bools)

	nacp := make(naImpl, len(bs.naImpl))
	copy(nacp, bs.naImpl)

	return &boolSeries{
		nameImpl: bs.nameImpl,
		bools:    vcp,
		naImpl:   nacp,
	}
}

func (bs *boolSeries) Type() Type            { return Bool }
func (bs *boolSeries) Bool(i int) bool       { return bs.bools[i] }
func (bs *boolSeries) SetBool(i int, v bool) { bs.bools[i] = v }

func (bs *boolSeries) Select(indexes []int) Series {
	v := make([]bool, len(indexes))
	na := make(naImpl, len(indexes))

	for i, index := range indexes {
		v[i] = bs.bools[index]
		na[i] = bs.naImpl[index]
	}
	return &boolSeries{
		nameImpl: bs.nameImpl,
		bools:    v,
		naImpl:   na,
	}
}

type numberSeries struct {
	disableBool
	disableString

	nameImpl
	naImpl
	numbers []float64
}

func NewNumberSeries(name string, nrow int) Series {
	return &numberSeries{
		nameImpl: nameImpl(name),
		naImpl:   make(naImpl, nrow),
		numbers:  make([]float64, nrow),
	}
}

func (fs *numberSeries) Copy() Series {
	vcp := make([]float64, len(fs.numbers))
	copy(vcp, fs.numbers)

	nacp := make(naImpl, len(fs.naImpl))
	copy(nacp, fs.naImpl)

	return &numberSeries{
		nameImpl: fs.nameImpl,
		numbers:  vcp,
		naImpl:   nacp,
	}
}

func (fs *numberSeries) Type() Type                 { return Number }
func (fs *numberSeries) Number(i int) float64       { return fs.numbers[i] }
func (fs *numberSeries) SetNumber(i int, v float64) { fs.numbers[i] = v }

func (fs *numberSeries) Select(indexes []int) Series {
	v := make([]float64, len(indexes))
	na := make(naImpl, len(indexes))

	for i, index := range indexes {
		v[i] = fs.numbers[index]
		na[i] = fs.naImpl[index]
	}
	return &numberSeries{
		nameImpl: fs.nameImpl,
		numbers:  v,
		naImpl:   na,
	}
}

type stringSeries struct {
	disableBool
	disableNumber

	nameImpl
	naImpl
	strings []string
}

func NewStringSeries(name string, nrow int) Series {
	return &stringSeries{
		nameImpl: nameImpl(name),
		naImpl:   make(naImpl, nrow),
		strings:  make([]string, nrow),
	}
}

func (ss *stringSeries) Copy() Series {
	vcp := make([]string, len(ss.strings))
	copy(vcp, ss.strings)

	nacp := make(naImpl, len(ss.naImpl))
	copy(nacp, ss.naImpl)

	return &stringSeries{
		nameImpl: ss.nameImpl,
		strings:  vcp,
		naImpl:   nacp,
	}
}

func (ss *stringSeries) Type() Type                { return String }
func (ss *stringSeries) String(i int) string       { return ss.strings[i] }
func (ss *stringSeries) SetString(i int, v string) { ss.strings[i] = v }

func (ss *stringSeries) Select(indexes []int) Series {
	v := make([]string, len(indexes))
	na := make(naImpl, len(indexes))

	for i, index := range indexes {
		v[i] = ss.strings[index]
		na[i] = ss.naImpl[index]
	}
	return &stringSeries{
		nameImpl: ss.nameImpl,
		strings:  v,
		naImpl:   na,
	}
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
