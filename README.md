# dataframe

[![GoDoc](https://godoc.org/github.com/awterman/dataframe?status.svg)](https://godoc.org/github.com/awterman/dataframe)&nbsp;
[![Go Report Card](https://goreportcard.com/badge/github.com/awterman/dataframe)](https://goreportcard.com/report/github.com/awterman/dataframe)

## Features
- Support *bool*, *number*, *string* and custom types.
- Seperate dataFrame implementation and operation functions, which means custom functions are easy to define.
- Less interfaces and pointers, higher performance.

## Design
- Operations on interfaces are expensive, so using interface casually should be avoided.
- Less method definitions, so series could be easier to customized.
