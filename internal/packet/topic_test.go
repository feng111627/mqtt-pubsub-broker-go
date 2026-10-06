package packet

import "testing"

func TestValidTopicName(t *testing.T) {
	cases := []struct {
		topic string
		want  bool
	}{
		{"a", true},
		{"a/b/c", true},
		{"a//b", true}, // empty level is significant and legal in a name
		{"/", true},    // two empty levels
		{"sport/tennis/player1", true},
		{"", false},    // empty name forbidden
		{"a/+", false}, // wildcard in name
		{"a/#", false},
		{"a+b", false},      // embedded plus
		{"a\x00b", false},   // NUL
		{"a\u0001b", false}, // forbidden control char
		{"house/中文", true},  // UTF-8 ok
	}
	for _, c := range cases {
		if got := ValidTopicName(c.topic); got != c.want {
			t.Errorf("ValidTopicName(%q) = %v, want %v", c.topic, got, c.want)
		}
	}
}

func TestValidFilter(t *testing.T) {
	cases := []struct {
		filter string
		want   bool
	}{
		{"#", true},
		{"+", true},
		{"+/+", true},
		{"a/+", true},
		{"a/#", true},
		{"sport/+/player1", true},
		{"#/b", false}, // # not last
		{"a#", false},  // embedded hash
		{"a+", false},  // embedded plus
		{"", false},
		{"a/\x00", false},
		{"a//+", true},
	}
	for _, c := range cases {
		if got := ValidFilter(c.filter); got != c.want {
			t.Errorf("ValidFilter(%q) = %v, want %v", c.filter, got, c.want)
		}
	}
}

func TestMatch(t *testing.T) {
	cases := []struct {
		filter, topic string
		want          bool
	}{
		// exact
		{"a/b", "a/b", true},
		{"a/b", "a/c", false},
		{"a/b", "a/b/c", false},
		// single level wildcard
		{"+", "a", true},
		{"+", "a/b", false},
		{"a/+", "a/b", true},
		{"a/+", "a/", true},
		{"a/+", "a/b/c", false},
		{"+/+", "a/b", true},
		{"sport/+/player1", "sport/tennis/player1", true},
		{"sport/+/player1", "sport/tennis/player2", false},
		// multi level wildcard
		{"#", "a/b/c", true},
		{"#", "a", true},
		{"a/#", "a", true}, // # matches zero levels
		{"a/#", "a/b/c", true},
		{"a/#", "b/c", false},
		{"a/#", "a/", true}, // empty final level
		// empty levels significant
		{"a//b", "a//b", true},
		{"a/+/b", "a//b", true},
		{"a/+/b", "a/x/b", true},
		{"a/+/b", "a/x/y/b", false},
		// case sensitivity
		{"A/B", "a/b", false},
	}
	for _, c := range cases {
		if got := Match(c.filter, c.topic); got != c.want {
			t.Errorf("Match(%q, %q) = %v, want %v", c.filter, c.topic, got, c.want)
		}
	}
}
