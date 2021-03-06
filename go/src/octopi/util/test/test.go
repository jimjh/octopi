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
	t.Errorf("%s: variable should be nil, but was %v.", prefix, x)
}

// AssertNotNil checks if the given object is not nil and logs an error with
// the given prefix.
func (t *Test) AssertNotNil(x interface{}, prefix string) {
	if nil != x {
		return
	}
	t.Errorf("%s: variable should not be nil.", prefix)
}

// AssertPositive checks if the given number is greater than zero.
func (t *Test) AssertPositive(x int64, prefix string) {
	if x <= 0 {
		t.Errorf("%s: variable should be positive.", prefix)
	}
}

// AssertTrue ensures that the given variable is true.
func (t *Test) AssertTrue(x bool, prefix string) {
	if !x {
		t.Errorf("%s: variable should be true.")
	}
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

// IntMatcher compares ints for equality.
type IntMatcher struct{}

func (m *IntMatcher) Match(expected interface{}, actual interface{}) bool {
	return expected.(int) == actual.(int)
}
