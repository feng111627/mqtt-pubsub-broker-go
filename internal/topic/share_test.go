package topic

import "testing"

func TestParseShared(t *testing.T) {
	cases := []struct {
		filter string
		ok     bool
		group  string
		inner  string
	}{
		{"$share/g1/a/b", true, "g1", "a/b"},
		{"$share/workers/jobs/+", true, "workers", "jobs/+"},
		{"$share/g/#", true, "g", "#"},
		{"a/b", false, "", ""},
		{"$share/", false, "", ""},
		{"$share/g1", false, "", ""},  // 缺内层过滤器
		{"$share//a", false, "", ""},  // 空组名
		{"$share/g1/", false, "", ""}, // 空内层
		{"$share/g1/a", true, "g1", "a"},
	}
	for _, tc := range cases {
		sf, ok := ParseShared(tc.filter)
		if ok != tc.ok {
			t.Errorf("ParseShared(%q) ok=%v want %v", tc.filter, ok, tc.ok)
			continue
		}
		if ok && (sf.Group != tc.group || sf.Filter != tc.inner) {
			t.Errorf("ParseShared(%q)=%+v want group=%q inner=%q", tc.filter, sf, tc.group, tc.inner)
		}
	}
}

func TestValidateShared(t *testing.T) {
	if err := ValidateShared(SharedFilter{Group: "g", Filter: "a/+"}); err != nil {
		t.Fatalf("valid shared filter rejected: %v", err)
	}
	if err := ValidateShared(SharedFilter{Group: "g#1", Filter: "a"}); err == nil {
		t.Fatal("wildcard in group name should be rejected")
	}
	if err := ValidateShared(SharedFilter{Group: "g", Filter: "a/#/b"}); err == nil {
		t.Fatal("invalid inner filter should be rejected")
	}
	if err := ValidateShared(SharedFilter{Group: "g", Filter: "$share/h/a"}); err == nil {
		t.Fatal("nested shared filter should be rejected")
	}
}
