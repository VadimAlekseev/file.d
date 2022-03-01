package devnull

import (
	"io/ioutil"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/ozontech/file.d/fd"
	"github.com/ozontech/file.d/pipeline"
	"go.uber.org/atomic"
)

/*{ introduction
It provides an API to test pipelines and other plugins.
}*/

const (
	outPluginType = "devnull"
	batchSize     = 128
)

type Plugin struct {
	controller pipeline.OutputPluginController
	outFn      func(event *pipeline.Event)
	total      *atomic.Int64

	avgEventSize int
	batcher      *pipeline.Batcher
	logger       *zap.SugaredLogger
	mu           *sync.RWMutex
}

type Config struct{}

type data struct {
	outBuf []byte
}

func init() {
	fd.DefaultPluginRegistry.RegisterOutput(&pipeline.PluginStaticInfo{
		Type:    outPluginType,
		Factory: Factory,
	})
}

func Factory() (pipeline.AnyPlugin, pipeline.AnyConfig) {
	return &Plugin{}, &Config{}
}

func (p *Plugin) Start(_ pipeline.AnyConfig, params *pipeline.OutputPluginParams) {
	p.controller = params.Controller
	p.logger = params.Logger
	p.total = &atomic.Int64{}

	p.mu = &sync.RWMutex{}

	p.batcher = pipeline.NewBatcher(
		params.PipelineName,
		outPluginType,
		p.out,
		nil,
		p.controller,
		8,
		batchSize,
		1*time.Second,
		0,
	)

	p.batcher.Start()
}

//! fn-list
//^ fn-list

//> It sets up a hook to make sure the test event passes successfully to output.
func (p *Plugin) SetOutFn(fn func(event *pipeline.Event)) { //*
	p.outFn = fn
}

func (p *Plugin) Stop() {
}

func (p *Plugin) Out(event *pipeline.Event) {
	if p.outFn != nil {
		p.outFn(event)
	}

	p.batcher.Add(event)
}

func (p *Plugin) out(workerData *pipeline.WorkerData, batch *pipeline.Batch) {
	if *workerData == nil {
		*workerData = &data{
			outBuf: make([]byte, 0, batchSize*p.avgEventSize),
		}
	}
	data := (*workerData).(*data)

	// handle too much memory consumption
	if cap(data.outBuf) > batchSize*p.avgEventSize {
		data.outBuf = make([]byte, 0, batchSize*p.avgEventSize)
	}

	outBuf := data.outBuf[:0]

	for _, event := range batch.Events {
		outBuf, _ = event.Encode(outBuf)
		outBuf = append(outBuf, byte('\n'))
	}
	data.outBuf = outBuf

	p.write(outBuf)
}

func (p *Plugin) write(data []byte) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if err := ioutil.WriteFile("/dev/null", data, 0644); err != nil {
		p.logger.Fatalf("could not write into the file: /dev/null, error: %s", err.Error())
	}
}
