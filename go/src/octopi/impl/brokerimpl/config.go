package brokerimpl

import (
	"octopi/util/config"
	"octopi/util/log"
	"os"
	"path/filepath"
	"strconv"
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

// If log dir is not given, default to a temporary directory.
var logDir = os.TempDir()

// LogDir returns the "log_dir" option in the configuration.
func (c *Config) LogDir() string {
	rel := filepath.Join(c.Base, c.Get("log_dir", logDir))
	abs, err := filepath.Abs(rel)
	if nil != err {
		panic(err)
	}
	return abs
}

// Host returns the host of this broker. Defaults to os.Hostname.
func (c *Config) Host() string {
	host, err := os.Hostname()
	if nil == err {
		return c.Get("host", host)
	}
	return c.Get("host", "localhost")
}

const default_port = 5050

// Port returns the port that this broker is listening on.
func (c *Config) Port() int {
	port, err := strconv.Atoi(c.Get("port", strconv.Itoa(default_port)))
	if nil != err {
		panic(err)
	}
	return port
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
