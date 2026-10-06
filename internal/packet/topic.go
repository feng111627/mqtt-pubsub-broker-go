package packet

import (
	"strings"
	"unicode/utf8"
)

// SplitTopic splits a topic name/filter into its level segments.
//
// The root topic "" splits into a single empty level (and is invalid for
// PUBLISH). Internal empty levels are preserved: "a//b" -> ["a","","b"].
func SplitTopic(topic string) []string {
	if topic == "" {
		return []string{""}
	}
	return strings.Split(topic, "/")
}

// validNameLevel enforces the MQTT 3.1.1/5.0 topic-name character rules:
// no wildcards and no forbidden control characters.
func validNameLevel(level string) bool {
	if strings.ContainsAny(level, "+#") {
		return false
	}
	return ValidateMQTTString([]byte(level)) == nil
}

// ValidTopicName checks a PUBLISH topic name against the MQTT rules:
// non-empty, no wildcard characters, no NUL, UTF-8 encodable.
func ValidTopicName(topic string) bool {
	if topic == "" {
		return false
	}
	if !utf8.ValidString(topic) {
		return false
	}
	for _, lv := range SplitTopic(topic) {
		if !validNameLevel(lv) {
			return false
		}
	}
	return true
}

// ValidFilter checks a subscription topic filter:
//   - levels may contain "+" only as the entire level;
//   - "#" may only be the final level and occupy it entirely
//     ("#" and "x/#" are legal, "a#" / "#/b" are not);
//   - no NUL bytes, non-empty.
func ValidFilter(filter string) bool {
	if filter == "" {
		return false
	}
	if !utf8.ValidString(filter) {
		return false
	}
	levels := SplitTopic(filter)
	for i, lv := range levels {
		if ValidateMQTTString([]byte(lv)) != nil {
			return false
		}
		switch {
		case lv == "+":
			// whole-level wildcard: always legal
		case lv == "#":
			if i != len(levels)-1 {
				return false // "#" must be last
			}
		case strings.ContainsAny(lv, "+#"):
			return false // embedded wildcard, e.g. "a+" or "a#b"
		}
	}
	return true
}

// Match reports whether a concrete topic name matches a filter.
//
// Rules (deterministic and used for every routing decision):
//   - filter validity is the caller's responsibility (ValidFilter);
//   - "+" matches exactly one level, including an empty level;
//   - "#" matches zero or more remaining levels ("sport/#", "sport/");
//   - the number of levels must agree unless "#" terminates the filter;
//   - comparison is byte-wise and case sensitive.
func Match(filter, topic string) bool {
	f := SplitTopic(filter)
	t := SplitTopic(topic)
	i := 0
	for ; i < len(f); i++ {
		switch f[i] {
		case "#":
			return true // matches zero+ remaining levels, including empty ones
		case "+":
			if i >= len(t) {
				return false
			}
			// any single level (including "") matches
		default:
			if i >= len(t) || f[i] != t[i] {
				return false
			}
		}
	}
	return i == len(t)
}
