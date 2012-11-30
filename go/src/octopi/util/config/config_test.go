package config

import (
	"encoding/json"
	"io/ioutil"
	"octopi/util/test"
	"os"
	"path/filepath"
	"testing"
)

// Default file permission
const perm = 0666

// TestReadJSON ensures that config can read options from a valid JSON file.
func TestReadJSON(tester *testing.T) {

	t := test.New(tester)

	dir := os.TempDir()
	config := new(Config)
	config.Options = map[string]string{
		"key":   "value",
		"mount": "everest",
		"port":  "123",
		"price": "0.0123",
	}

	name := filepath.Join(dir, "config.json")
	data, err := json.Marshal(config)
	t.AssertNil(err, "json.Marshal")

	t.AssertNil(ioutil.WriteFile(name, data, perm), "ioutil.WriteFile")
	defer os.Remove(name)

	actual, err := Init(name)
	t.AssertNil(err, "config.Init")

	matcher := new(configMatcher)
	t.AssertEqual(matcher, config.Options, actual.Options)

}

// TestInvalidJSON ensures that Init returns an error if the JSON file is
// invalid.
func TestInvalidJSON(tester *testing.T) {

	t := test.New(tester)

	dir := os.TempDir()
	name := filepath.Join(dir, "config.json")

	t.AssertNil(ioutil.WriteFile(name, []byte("xyz"), perm), "ioutil.WriteFile")
	defer os.Remove(name)

	_, err := Init(name)
	t.AssertNotNil(err, "Init")

}

// TestGet ensures that Get can get all the valid keys.
func TestGet(tester *testing.T) {

	t := test.New(tester)

	config := new(Config)
	config.Options = map[string]string{
		"key":   "value",
		"mount": "everest",
		"port":  "123",
		"price": "0.0123",
	}

	matcher := new(test.StringMatcher)
	for k, v := range config.Options {
		t.AssertEqual(matcher, config.Get(k), v)
	}
}

// TestPanic ensures that Get panics on missing keys.
func TestPanic(tester *testing.T) {

	t := test.New(tester)

	config := new(Config)
	config.Options = map[string]string{
		"key":   "value",
		"mount": "everest",
		"port":  "123",
		"price": "0.0123",
	}

	defer func() {
		r := recover()
		t.AssertNotNil(r, "config.Get")
	}()
	config.Get("x")

}

// TestDefault ensures that Get can fall back on default values.
func TestDefault(tester *testing.T) {

	t := test.New(tester)

	config := new(Config)
	config.Options = map[string]string{
		"key":   "value",
		"mount": "everest",
		"port":  "123",
		"price": "0.0123",
	}

	defer func() {
		r := recover()
		t.AssertNil(r, "config.Get")
	}()

	matcher := new(test.StringMatcher)
	t.AssertEqual(matcher, "y", config.Get("x", "y"))

}

type configMatcher struct {
}

// match returns true iff the two args are both maps, and every k,v pair in the
// expected map exists in the actual map.
func (m *configMatcher) Match(expected interface{}, actual interface{}) bool {

	emap := expected.(map[string]string)
	amap := actual.(map[string]string)

	for k, v := range emap {
		if amap[k] != v {
			return false
		}
	}

	return true

}
