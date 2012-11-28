package brokerimpl

import (
	"octopi/util/config"
)

// Configuration options for broker.
type Config struct {
	config.Config
}

// Register returns the "register" option in the configuration.
func (c *Config) Register() string {
	return c.Get("register")
}

// LogDir returns the "log_dir" option in the configuration.
func (c *Config) LogDir() string {
	return c.Get("log_dir")
}
