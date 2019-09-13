package dataframe

import "strings"

type optionValues struct {
	formatter formatter
}

func defaultFormatter(df DataFrame) string {
	builder := strings.Builder{}
	for _, s := range df.GetAllSeries() {
		builder.WriteString(s.Name())
		builder.WriteString(": ")
		builder.WriteString(s.String())
		builder.WriteByte('\n')
	}
	return builder.String()
}

func newOptionValues() *optionValues {
	return &optionValues{
		formatter: defaultFormatter,
	}
}

type Option func(*optionValues)

func WithFormatter(f formatter) Option {
	return func(ov *optionValues) {
		ov.formatter = f
	}
}
