// Package log implements a simple logger with severity levels and colors. It
// also has a predefined standard logger, which is easier to use than creating
// a Logger manually. The Fatal functions call os.Exit(1) after writing the log
// message. The Panic functions call panic after writing the log message.
package log

import (
	"fmt"
	"io"
	"log"
	"os"
)

// Severity levels
const (
	DEBUG = iota
	INFO
	WARN
	ERROR
	PANIC
	FATAL
)

// Output colors
const (
	FG_GREEN   = "\x1b[32m"
	FG_YELLOW  = "\x1b[33m"
	FG_MAGENTA = "\x1b[35m"
	FG_RED     = "\x1b[31m"
	BG_RED     = "\x1b[41m"
)

// Logger inherits from the standard logger.
type Logger struct {
	log.Logger     // anonymous field
	severity   int // severity level
}

// Default severity level
const DEFAULT_SEVERITY int = WARN

// New creates a new Logger. The out variable sets the destination to which log
// data will be written. The prefix appears at the beginning of each generated
// log line. The flag argument defines the logging properties.
func New(out io.Writer, prefix string, flag int) *Logger {
	return &Logger{*log.New(out, prefix, flag), DEFAULT_SEVERITY}
}

// SetVerbose configures the desired severity level. Log messages below this
// level are not printed.
func (l *Logger) SetVerbose(verbose int) {
	l.severity = verbose
}

// Logs a debug message.
func (l *Logger) Debug(format string, v ...interface{}) {
	l.printf(DEBUG, "D -- "+format, v...)
}

// Logs an informational message.
func (l *Logger) Info(format string, v ...interface{}) {
	l.printf(INFO, FG_GREEN+"I -- "+format+"\x1b[0m", v...)
}

// Logs a warning message.
func (l *Logger) Warn(format string, v ...interface{}) {
	l.printf(WARN, FG_YELLOW+"W -- "+format+"\x1b[0m", v...)
}

// Logs an error message.
func (l *Logger) Error(format string, v ...interface{}) {
	l.printf(ERROR, FG_MAGENTA+"E -- "+format+"\x1b[0m", v...)
}

// Logs message and calls panic()
func (l *Logger) Panic(format string, v ...interface{}) {
	l.printf(PANIC, FG_RED+"P -- "+format+"\x1b[0m", v...)
	panic(fmt.Sprintf(format, v...))
}

// Logs message and calls os.Exit(1)
func (l *Logger) Fatal(format string, v ...interface{}) {
	l.printf(FATAL, BG_RED+"F -- "+format+"\x1b[0m", v...)
	os.Exit(1)
}

func (l *Logger) printf(severity int, format string, v ...interface{}) {

	if severity < l.severity {
		return
	}

	l.Printf(format, v...)

}

// Predefined standard logger.
var logger *Logger = New(os.Stderr, "", log.LstdFlags)

func Debug(format string, v ...interface{}) {
	logger.Debug(format, v...)
}

func Info(format string, v ...interface{}) {
	logger.Info(format, v...)
}

func Warn(format string, v ...interface{}) {
	logger.Warn(format, v...)
}

func Error(format string, v ...interface{}) {
	logger.Error(format, v...)
}

func Panic(format string, v ...interface{}) {
	logger.Panic(format, v...)
}

func Fatal(format string, v ...interface{}) {
	logger.Fatal(format, v...)
}

func SetOutput(w io.Writer) {
	logger = New(w, logger.Prefix(), logger.Flags())
}

func SetPrefix(prefix string) {
	logger.SetPrefix(prefix)
}

func SetVerbose(verbose int) {
	logger.SetVerbose(verbose)
}
