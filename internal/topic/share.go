// 共享订阅（Shared Subscriptions）支持。
//
// 语法（MQTT 5.0 §4.8.2 的写法，本代理同时对 3.1.1 连接开放）：
//
//	$share/<共享组名>/<主题过滤器>
//
// 同一共享组内多个成员订阅时，一条消息在组内只投递给其中一个成员。
package topic

import (
	"fmt"
	"strings"
)

// SharedFilter 解析后的共享订阅过滤器。
type SharedFilter struct {
	Group  string // 共享组名
	Filter string // 组内实际使用的主题过滤器
}

// ParseShared 判断过滤器是否为共享订阅；是则返回解析结果。
// 仅识别 "$share/<group>/<filter>"：必须有非空组名与非空内层过滤器。
// 以 "$share" 为普通主题名的发布不受影响（只对订阅过滤器解析）。
func ParseShared(filter string) (SharedFilter, bool) {
	if !strings.HasPrefix(filter, "$share/") {
		return SharedFilter{}, false
	}
	rest := filter[len("$share/"):]
	g, inner, ok := strings.Cut(rest, "/")
	if !ok || g == "" || inner == "" {
		return SharedFilter{}, false
	}
	return SharedFilter{Group: g, Filter: inner}, true
}

// ValidateShared 校验共享订阅：组名合法且内层过滤器是合法的普通过滤器。
// 组名不允许通配符、NUL，也不允许再次嵌套 $share。
func ValidateShared(sf SharedFilter) error {
	if strings.ContainsAny(sf.Group, "+#") {
		return fmt.Errorf("shared group name %q must not contain wildcards", sf.Group)
	}
	if strings.ContainsRune(sf.Group, 0) {
		return fmt.Errorf("shared group name %q contains NUL", sf.Group)
	}
	if inner, ok := ParseShared(sf.Filter); ok {
		_ = inner
		return fmt.Errorf("shared filter must not be nested")
	}
	return ValidateFilter(sf.Filter)
}
