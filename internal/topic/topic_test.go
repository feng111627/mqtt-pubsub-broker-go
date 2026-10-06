package topic

import "testing"

func TestMatch(t *testing.T) {
	cases := []struct {
		filter, topic string
		want          bool
	}{
		// 精确匹配
		{"a/b/c", "a/b/c", true},
		{"a/b/c", "a/b/d", false},
		{"a/b", "a/b/c", false},
		{"a/b/c", "a/b", false},
		// 单级通配符
		{"a/+/c", "a/b/c", true},
		{"a/+/c", "a/b/d", false},
		{"a/+", "a/b", true},
		{"a/+", "a/b/c", false},
		{"+", "a", true},
		{"+", "a/b", false},
		{"a/+", "a/", true},    // + 可匹配空层级
		{"a//c", "a//c", true}, // 空层级精确匹配
		// 多级通配符
		{"a/#", "a", true},
		{"a/#", "a/b", true},
		{"a/#", "a/b/c", true},
		{"a/#", "b/c", false},
		{"#", "a/b/c", true},
		{"a/b/#", "a/b", true},
		{"a/b/#", "a/b/c/d", true},
		{"a/b/#", "a/c", false},
		// 系统主题保护
		{"#", "$SYS/broker", false},
		{"+/x", "$SYS/x", false},
		{"$SYS/#", "$SYS/broker", true},
		// 空输入
		{"", "a", false},
		{"a", "", false},
	}
	for _, c := range cases {
		if got := Match(c.filter, c.topic); got != c.want {
			t.Errorf("Match(%q, %q) = %v, want %v", c.filter, c.topic, got, c.want)
		}
	}
}

func TestValidateFilter(t *testing.T) {
	valid := []string{"a/b", "a/+/c", "a/#", "#", "+", "+/+", "a/+/#"}
	for _, f := range valid {
		if err := ValidateFilter(f); err != nil {
			t.Errorf("ValidateFilter(%q) unexpected error: %v", f, err)
		}
	}
	invalid := []string{"", "a#", "a/#/b", "a/b#", "a+", "a/+b", "a/\x00b"}
	for _, f := range invalid {
		if err := ValidateFilter(f); err == nil {
			t.Errorf("ValidateFilter(%q) expected error, got nil", f)
		}
	}
}

func TestValidatePublish(t *testing.T) {
	valid := []string{"a/b", "a//c", "$SYS/x"}
	for _, tp := range valid {
		if err := ValidatePublish(tp); err != nil {
			t.Errorf("ValidatePublish(%q) unexpected error: %v", tp, err)
		}
	}
	invalid := []string{"", "a/+", "a/#", "a/\x00b"}
	for _, tp := range invalid {
		if err := ValidatePublish(tp); err == nil {
			t.Errorf("ValidatePublish(%q) expected error, got nil", tp)
		}
	}
}
