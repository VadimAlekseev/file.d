package throttle

import (
	"fmt"
	"math/rand"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ozontech/file.d/cfg"
	"github.com/ozontech/file.d/logger"
	"github.com/ozontech/file.d/pipeline"
	"github.com/ozontech/file.d/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	insaneJSON "github.com/vitkovskii/insane-json"
	"go.uber.org/zap"
)

type testConfig struct {
	t           *testing.T
	config      *Config
	eventsTotal int
	workTime    time.Duration
}

var formats = []string{
	`{"time":"%s","k8s_ns":"ns_1","k8s_pod":"pod_1"}`,
	`{"time":"%s","k8s_ns":"ns_2","k8s_pod":"pod_2"}`,
	`{"time":"%s","k8s_ns":"not_matched","k8s_pod":"pod_3"}`,
}

func goid() int {
	var buf [64]byte
	n := runtime.Stack(buf[:], false)
	idField := strings.Fields(strings.TrimPrefix(string(buf[:n]), "goroutine "))[0]
	id, err := strconv.Atoi(idField)
	if err != nil {
		panic(fmt.Sprintf("cannot get goroutine id: %v", err))
	}
	return id
}

func (c *testConfig) runPipeline() int {
	logger.Level.SetLevel(zap.PanicLevel)
	p, input, output := test.NewPipelineMock(test.NewActionPluginStaticInfo(factory, c.config, pipeline.MatchModeAnd, nil, false))

	var outEvents int
	countByRule := map[string]int{}
	mx := &sync.RWMutex{}

	output.SetOutFn(func(e *pipeline.Event) {
		mx.Lock()
		defer mx.Unlock()

		pod := e.Root.Dig("k8s_ns").AsString()
		countByRule[pod]++
		if pod != "not_matched" && pod != "ns_2" && pod != "ns_1" {
			panic(fmt.Errorf("invalid enum: %s 3", pod))
		}

		fmt.Println(e.Root.EncodeToString())

		outEvents++
	})

	sourceNames := []string{
		`source_1`,
		`source_2`,
		`source_3`,
	}

	startTime := time.Now()
	for {
		index := rand.Int() % len(formats)
		// Format like RFC3339Nano, but nanoseconds are zero-padded, thus all times have equal length.
		json := fmt.Sprintf(formats[index], time.Now().UTC().Format("2006-01-02T15:04:05.000000000Z07:00"))
		input.In(10, sourceNames[rand.Int()%len(sourceNames)], 0, []byte(json))
		if time.Since(startTime) > c.workTime {
			break
		}
	}

	p.Stop()

	//tnow := time.Now()
	for {
		if c.eventsTotal <= outEvents {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	mx.Lock()
	defer mx.Unlock()
	fmt.Printf("countByRule: %#v\n", countByRule)

	time.Sleep(time.Second * 5)

	return outEvents
}

func TestInsaneJSON_Dig(t *testing.T) {
	original, err := insaneJSON.DecodeString(formats[0])
	require.NoError(t, err)

	expect := original.Dig("k8s_ns").AsString()

	stop := make(chan struct{})

	for i := 0; i < 10; i++ {
		go func() {
			for {
				select {
				case <-stop:
					return
				default:
					fake, err := insaneJSON.DecodeString(formats[2])
					require.NoError(t, err)
					insaneJSON.Release(fake)
				}
			}
		}()
	}

	for i := 0; i < 1_000_000; i++ {
		runtime.Gosched()
		current, err := insaneJSON.DecodeString(formats[0])
		//runtime.Gosched()
		require.NoError(t, err)
		//runtime.Gosched()
		require.Equal(t, expect, current.Dig("k8s_ns").AsString())
		//runtime.Gosched()
		insaneJSON.Release(current)
	}

	close(stop)
}

func TestThrottle(t *testing.T) {
	buckets := 2
	limitA := 2
	limitB := 3
	defaultLimit := 20

	iterations := 5

	totalBuckets := iterations + 1
	defaultLimitDelta := totalBuckets * defaultLimit
	eventsTotal := totalBuckets*(limitA+limitB) + defaultLimitDelta

	config := &Config{
		Rules: []RuleConfig{
			{Limit: int64(limitA), Conditions: map[string]string{"k8s_ns": "ns_1"}},
			{Limit: int64(limitB), Conditions: map[string]string{"k8s_ns": "ns_2"}},
		},
		BucketsCount:   buckets,
		BucketInterval: "100ms",
		ThrottleField:  "k8s_pod",
		TimeField:      "",
		DefaultLimit:   int64(defaultLimit),
	}
	err := cfg.Parse(config, nil)
	if err != nil {
		logger.Panic(err.Error())
	}

	workTime := config.BucketInterval_ * time.Duration(iterations)

	tconf := testConfig{t, config, eventsTotal, workTime}
	outEvents := tconf.runPipeline()

	if !assert.Equal(t, eventsTotal, outEvents) {
		for k, m := range limiters {
			for s, l := range m {
				fmt.Printf("%s: %s: %+v\n", k, s, l.buckets)
			}
		}
	}
}

func TestSizeThrottle(t *testing.T) {
	buckets := 4
	sizeFraction := 100
	limitA := sizeFraction * 2
	limitB := sizeFraction * 3
	defaultLimit := sizeFraction * 20

	dateLen := len("2006-01-02T15:04:05.999999999Z")
	iterations := 5

	totalBuckets := iterations + 1
	eventsPerBucket := limitA/(len(formats[0])+dateLen-2) + limitB/(len(formats[1])+dateLen-2) + defaultLimit/(len(formats[2])+dateLen-2)
	eventsTotal := totalBuckets * eventsPerBucket

	config := &Config{
		Rules: []RuleConfig{
			{Limit: int64(limitA), LimitKind: "size", Conditions: map[string]string{"k8s_ns": "ns_1"}},
			{Limit: int64(limitB), LimitKind: "size", Conditions: map[string]string{"k8s_ns": "ns_2"}},
		},
		BucketsCount:   buckets,
		BucketInterval: "100ms",
		ThrottleField:  "k8s_pod",
		TimeField:      "",
		DefaultLimit:   int64(defaultLimit),
		LimitKind:      "size",
	}
	err := cfg.Parse(config, nil)
	if err != nil {
		logger.Panic(err.Error())
	}

	workTime := config.BucketInterval_ * time.Duration(iterations)

	tconf := testConfig{t, config, eventsTotal, workTime}
	outEvents := tconf.runPipeline()

	assert.Equal(t, eventsTotal, outEvents)
}

func TestMixedThrottle(t *testing.T) {
	buckets := 2
	avgMessageSize := 90
	limitA := 2
	limitB := avgMessageSize * 3
	defaultLimit := 20

	iterations := 5

	totalBuckets := iterations + 1
	defaultLimitDelta := totalBuckets * defaultLimit
	eventsTotal := totalBuckets*(limitA+(limitB/avgMessageSize)) + defaultLimitDelta

	config := &Config{
		Rules: []RuleConfig{
			{Limit: int64(limitA), Conditions: map[string]string{"k8s_ns": "ns_1"}},
			{Limit: int64(limitB), LimitKind: "size", Conditions: map[string]string{"k8s_ns": "ns_2"}},
		},
		BucketsCount:   buckets,
		BucketInterval: "100ms",
		ThrottleField:  "k8s_pod",
		TimeField:      "",
		DefaultLimit:   int64(defaultLimit),
	}
	err := cfg.Parse(config, nil)
	if err != nil {
		logger.Panic(err.Error())
	}

	workTime := config.BucketInterval_ * time.Duration(iterations)

	tconf := testConfig{t, config, eventsTotal, workTime}
	outEvents := tconf.runPipeline()

	assert.Equal(t, eventsTotal, outEvents)
}
