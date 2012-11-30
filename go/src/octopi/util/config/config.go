// Package config provides some utility methods for reading options from
// configuration files. The current implementation only supports a single-level
// JSON object whose values are all strings.
//
// Example:
//    {
//      "Options": {
//        "port": "12345"
//      }
//    }
package config

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
)

// Config represents a set of configuration options.
type Config struct {
	Options map[string]string // key-value option pairs
}

// Init initializes a config struct from the given JSON file.
//
// Example:
//    config := c.Init("conf.json")
func Init(filename string) (*Config, error) {

	config := &Config{make(map[string]string)}
	data, err := ioutil.ReadFile(filename)
	if nil != err {
		return nil, err
	}

	err = json.Unmarshal(data, config)
	return config, err

}

// Get returns the configuration value from `Init` with the given key. Defaults
// to `defs[0]`.
//
// Example:
//    c.Get("port", "12345")  // defaults to "12345"
//    c.Get("x")              // panics if "x" is not found
func (c *Config) Get(key string, defs ...string) string {

	value, exists := c.Options[key]
	if exists {
		return value
	}

	if 0 < len(defs) {
		return defs[0]
	}

	panic(fmt.Sprintf("Property \"%s\" is required.", key))

}
