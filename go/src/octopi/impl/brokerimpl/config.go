package brokerimpl

import (
	"octopi/util/config"
	"octopi/util/log"
)

const (
	LEADER = iota
	FOLLOWER
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

// Role returns either "follower" or "leader"
func (c *Config) Role() int {
	role := c.Get("role", "follower")
	switch role {
	case "follower":
		return FOLLOWER
	case "leader":
		return LEADER
	default:
		log.Panic("Invalid role: %s.", role)
	}
	return LEADER
}
