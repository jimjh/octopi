// Package test provides some utility methods and functions for testing.
package test

import (
	"testing"
)

// Test inherits from testing.T and provides utility methods/functions.
type Test struct {
	*testing.T
}

// New creates a new test.
func New(t *testing.T) *Test {
	return &Test{t}
}

// AssertNil checks if the given error is nil and logs an error with the given
// prefix.
func (t *Test) AssertNil(x interface{}, prefix string) {
	if nil == x {
		return
	}
	t.Error(prefix, x)
}

// AssertNotNil checks if the given object is not nil and logs an error with
// the given prefix.
func (t *Test) AssertNotNil(x interface{}, prefix string) {
	if nil != x {
		return
	}
	t.Error(prefix, x)
}

// AssertEqual uses the given matcher to compare two values. It logs an error
// if the matcher returns false.
func (t *Test) AssertEqual(matcher Matcher,
	expected interface{}, actual interface{}) {

	if matcher.Match(expected, actual) {
		return
	}

	t.Errorf("Expected %v, was %v.", expected, actual)

}

// Matcher interface
type Matcher interface {
	Match(interface{}, interface{}) bool
}

// StringMatcher compares strings for equality.
type StringMatcher struct{}

func (m *StringMatcher) Match(expected interface{}, actual interface{}) bool {
	estr := expected.(string)
	astr := actual.(string)
	return estr == astr
}
