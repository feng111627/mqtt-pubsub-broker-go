// Package topic 提供 MQTT 主题名校验与通配符（+、#）匹配。
package topic

import (
	"fmt"
	"strings"
)

// ValidatePublish 校验发布主题名（不允许通配符）。
func ValidatePublish(topic string) error {
	if topic == "" {
		return fmt.Errorf("topic name must not be empty")
	}
	if strings.ContainsAny(topic, "+#") {
		return fmt.Errorf("publish topic %q must not contain wildcards", topic)
	}
	if strings.ContainsRune(topic, 0) {
		return fmt.Errorf("topic %q contains NUL character", topic)
	}
	if len(topic) > 65535 {
		return fmt.Errorf("topic name too long")
	}
	return nil
}

// ValidateFilter 校验订阅过滤器（允许 + 与 #，但位置必须合法）。
func ValidateFilter(filter string) error {
	if filter == "" {
		return fmt.Errorf("topic filter must not be empty")
	}
	if strings.ContainsRune(filter, 0) {
		return fmt.Errorf("filter %q contains NUL character", filter)
	}
	if len(filter) > 65535 {
		return fmt.Errorf("topic filter too long")
	}
	levels := strings.Split(filter, "/")
	for i, lv := range levels {
		if strings.Contains(lv, "#") {
			// # 必须独占一层且为最后一层
			if lv != "#" {
				return fmt.Errorf("filter %q: '#' must occupy an entire level", filter)
			}
			if i != len(levels)-1 {
				return fmt.Errorf("filter %q: '#' must be the last level", filter)
			}
		}
		if strings.Contains(lv, "+") && lv != "+" {
			return fmt.Errorf("filter %q: '+' must occupy an entire level", filter)
		}
	}
	return nil
}

// Match 判断订阅过滤器是否命中主题名。
// 命中与不命中的判定是确定性的纯函数：
//   - "+" 匹配且仅匹配一个层级（可为空串层级）
//   - "#" 匹配其自身及之后任意多个层级，且可匹配父级（"a/#" 命中 "a"）
//   - 以 "$" 开头的系统主题不被以通配符开头的过滤器命中
func Match(filter, topic string) bool {
	if filter == "" || topic == "" {
		return false
	}
	// 系统主题保护：$ 开头的主题不匹配通配符开头的过滤器
	if topic[0] == '$' && (filter[0] == '+' || filter[0] == '#') {
		return false
	}
	flevels := strings.Split(filter, "/")
	tlevels := strings.Split(topic, "/")

	for i, fl := range flevels {
		if fl == "#" {
			// # 为最后一层（ValidateFilter 保证），命中剩余所有层级，
			// 包括层级数恰好相等的情况（"a/#" 命中 "a"）。
			return true
		}
		if i >= len(tlevels) {
			return false
		}
		if fl == "+" {
			continue
		}
		if fl != tlevels[i] {
			return false
		}
	}
	// 过滤器不含 # 时，层级数必须完全一致
	return len(flevels) == len(tlevels)
}
