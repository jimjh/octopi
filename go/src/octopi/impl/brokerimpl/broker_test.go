package brokerimpl

import (
	"octopi/util/config"
	"os"
	"strconv"
	"testing"
)

func newTestConfig() *Config {
	options := &config.Config{make(map[string]string)}
	options.Options["log_dir"] = os.TempDir()
	options.Options["register"] = ""
	return &Config{*options}
}

// TestTails checks that the tails function returns the sizes of all log files.
func TestTails(t *testing.T) {

	config := newTestConfig()
	expected := make(map[string]int64, 10)

	for i := 0; i < 10; i++ {
		name := strconv.Itoa(i)
		log, err := OpenLog(config, name, 0)
		log.WriteNext(new(LogEntry))
		if nil != err {
			t.Fatal("Unable to open log file.")
		}
		expected[name] = 40
		log.Close()
	}

	broker, _ := New(&config.Config)
	tails := broker.tails()
	for topic, size := range tails {
		if expected[topic] != size {
			t.Fatalf("Expected %s to have size %d, was %d.", expected[topic], size)
		}
		delete(expected, topic)
	}

	if 0 != len(expected) {
		t.Fatal("Tails did not report all log files.")
	}

}
