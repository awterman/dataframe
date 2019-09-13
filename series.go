package dataframe

type Type int

const (
	None Type = iota
	Bool
	Number
	String
)

type Series interface {
	Type() Type
	Name() string
	Rename(s string)
	Len() int
	Copy() Series
	Select(indexed []int) Series
	String() string

	GetBool(i int) bool
	SetBool(i int, v bool)

	GetNumber(i int) float64
	SetNumber(i int, v float64)

	GetString(i int) string
	SetString(i int, v string)

	IsNA(i int) bool
	SetNA(i int)
}

type disableBool struct{}
type disableNumber struct{}
type disableString struct{}
type nameImpl string
type naImpl []bool

func (disableBool) GetBool(i int) bool           { panic("disabled") }
func (disableBool) SetBool(i int, v bool)        { panic("disabled") }
func (disableNumber) GetNumber(i int) float64    { panic("disabled") }
func (disableNumber) SetNumber(i int, v float64) { panic("disabled") }
func (disableString) GetString(i int) string     { panic("disabled") }
func (disableString) SetString(i int, v string)  { panic("disabled") }

func (n *nameImpl) Name() string    { return string(*n) }
func (n *nameImpl) Rename(s string) { *n = nameImpl(s) }

func (na naImpl) IsNA(i int) bool { return na[i] }
func (na naImpl) SetNA(i int)     { na[i] = true }
func (na naImpl) Len() int        { return len(na) }

type CustomSeries struct {
	disableBool
	disableNumber
	disableString
}

func NewSeries(t Type, name string, nrow int) Series {
	var f func(name string, nrow int) Series

	switch t {
	case None:
		f = NewPlaceholderSeries
	case Bool:
		f = NewBoolSeries
	case Number:
		f = NewNumberSeries
	case String:
		f = NewStringSeries
	default:
		panic("unknown type: " + string(t))
	}

	return f(name, nrow)
}
