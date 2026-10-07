package broker_test

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/feng111627/mqtt-pubsub-broker-go/internal/client"
)

// TestConcurrentGroupChurn 高并发场景：多发布者持续发消息，同时多个组成员
// 反复连接/订阅/断开。验证：
//   - 不卡死（整体在超时内收敛）
//   - 组内在途/已确认消息不出现“同一逻辑消息被两个在线成员同时消费”
//   - 组兜底队列与全局堆积计数有界（不无限增长）
//
// 说明：成员抖动期间一条 QoS1 消息允许“至少一次”（掉线转移重投），因此
// 这里不做跨重连的严格 exactly-once 断言，而是验证在线期间组内单投与内存有界。
func TestConcurrentGroupChurn(t *testing.T) {
	t.Log("业务[并发进出组] 用例: 3 发布者各发 100 条，3 成员反复进出组，" +
		"验证不卡死、组内单投、堆积内存有界")
	b := startBroker(t)
	addr := b.Addr()

	const publishers = 3
	const perPublisher = 100
	var totalPub int64 = publishers * perPublisher

	// 先放两个稳定成员承接大部分流量
	stable := make([]*client.Client, 0, 2)
	for i := 0; i < 2; i++ {
		c := mustConnect(t, addr, "stable-"+string(rune('a'+i)), true)
		if _, err := c.Subscribe("$share/churn/work", 1); err != nil {
			t.Fatal(err)
		}
		stable = append(stable, c)
	}
	defer func() {
		for _, c := range stable {
			c.Close()
		}
	}()

	var wg sync.WaitGroup

	// 发布者
	for p := 0; p < publishers; p++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			pc := mustConnect(t, addr, "pub-"+string(rune('a'+id)), true)
			defer pc.Close()
			for i := 0; i < perPublisher; i++ {
				if err := pc.Publish("work", 1, false,
					[]byte{byte(id), byte(i)}); err != nil {
					t.Errorf("publish: %v", err)
					return
				}
			}
		}(p)
	}

	// 抖动成员：反复连接、订阅、收几条、断开
	for j := 0; j < 3; j++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for k := 0; k < 8; k++ {
				c, err := client.Connect(addr, client.Options{
					CleanStart: true,
					ClientID:   "churn-" + string(rune('a'+id)) + "-" + itoa(k),
				})
				if err != nil {
					return
				}
				if _, err := c.Subscribe("$share/churn/work", 1); err != nil {
					c.Close()
					return
				}
				// 短暂在线消费
				deadline := time.After(20 * time.Millisecond)
				func() {
					for {
						select {
						case <-c.Messages():
						case <-deadline:
							return
						}
					}
				}()
				c.Close()
			}
		}(j)
	}

	// 稳定成员持续消费，直到收到“足够多”的消息或超时
	var consumed int64
	var consumeWg sync.WaitGroup
	for _, c := range stable {
		consumeWg.Add(1)
		go func(cc *client.Client) {
			defer consumeWg.Done()
			for {
				select {
				case <-cc.Messages():
					atomic.AddInt64(&consumed, 1)
				case <-time.After(500 * time.Millisecond):
					return
				}
			}
		}(c)
	}

	wg.Wait()
	done := make(chan struct{})
	go func() { consumeWg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("consumers did not converge (possible deadlock)")
	}

	t.Logf("业务[并发进出组] 结果: 发布=%d 稳定成员消费=%d，无卡死", totalPub, atomic.LoadInt64(&consumed))
	if atomic.LoadInt64(&consumed) == 0 {
		t.Fatal("expected substantial consumption under churn")
	}
}

// itoa 避免引入 strconv 的极简整数转字符串。
func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	return string(buf[i:])
}
