package playground

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"runtime"
	"strings"
	"time"

	"github.com/bitly/go-simplejson"
	"github.com/go-jose/go-jose/v3/json"
	jsoniter "github.com/json-iterator/go"
	"github.com/ozontech/file.d/fd"
	"github.com/ozontech/file.d/pipeline"
	"github.com/ozontech/file.d/plugin/output/devnull"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	"go.uber.org/atomic"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"sigs.k8s.io/yaml"
)

var jsoniterConfig = jsoniter.ConfigCompatibleWithStandardLibrary

const (
	pipelineCapacity = 1
)

type DoActionsRequest struct {
	Actions []json.RawMessage `json:"actions"`
	Events  []json.RawMessage `json:"events"`
	Debug   bool              `json:"debug"`
}

type ProcessResult struct {
	Event json.RawMessage `json:"event"`
}

type DoActionsResponse struct {
	Result  []ProcessResult `json:"result"`
	Stdout  string          `json:"stdout"`
	Metrics string          `json:"metrics"`
}

type DoActionsHandler struct {
	plugins  *fd.PluginRegistry
	logger   *zap.Logger
	requests atomic.Int64
}

var _ http.Handler = (*DoActionsHandler)(nil)

func NewDoActionsHandler(plugins *fd.PluginRegistry, logger *zap.Logger) *DoActionsHandler {
	return &DoActionsHandler{plugins: plugins, logger: logger}
}

func (h *DoActionsHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "", http.StatusMethodNotAllowed)
		return
	}

	req, err := h.unmarshalRequest(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if len(req.Events) > 32 || len(req.Events) == 0 || len(req.Actions) > 64 {
		http.Error(w, "validate error: events count must be in range [0, 32] and actions count [0, 64]", http.StatusBadRequest)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), time.Second*2)
	defer cancel()

	resp, code, err := h.doActions(ctx, req)
	if err != nil {
		http.Error(w, fmt.Sprintf("do actions: %s", err.Error()), code)
		return
	}
	_ = json.NewEncoder(w).Encode(resp)
}

func (h *DoActionsHandler) doActions(ctx context.Context, req DoActionsRequest) (resp DoActionsResponse, code int, err error) {
	if req.Debug {
		// wrap all plugins with the debug plugin
		newActions := make([]json.RawMessage, 0, len(req.Actions)*2)

		for _, action := range req.Actions {
			// trying to get the name of the next action
			var actionWithType = struct {
				Type string `json:"type"`
			}{}
			if err := jsoniterConfig.Unmarshal(action, &actionWithType); err != nil {
				panic(err)
			}
			actionType := jsonEscape(actionWithType.Type)

			// wrap plugin with the debug actions
			debugActionBefore := json.RawMessage(fmt.Sprintf(`{"type": "debug", "message": "before %s"}`, actionType))
			debugActionAfter := json.RawMessage(fmt.Sprintf(`{"type": "debug", "message": "after %s"}`, actionType))
			newActions = append(newActions, debugActionBefore, action, debugActionAfter)
		}

		req.Actions = newActions
	}

	// pipeline stdout buffer
	stdoutBuf := new(bytes.Buffer)
	stdoutBuf.Grow(1 << 10)
	stdout := preparePipelineLogger(stdoutBuf)

	metricsRegistry := prometheus.NewRegistry()

	p := pipeline.New("test", &pipeline.Settings{
		Decoder:             "json",
		Capacity:            pipelineCapacity,
		MaintenanceInterval: time.Millisecond * 100,
		EventTimeout:        time.Millisecond * 100,
		AntispamThreshold:   0,
		IsStrict:            false,
	}, metricsRegistry, stdout)

	// callback to collect output events
	events := make(chan json.RawMessage, len(req.Events))
	outputCb := func(event *pipeline.Event) {
		events <- event.Root.EncodeToByte()
	}

	if err := h.setupPipeline(p, req, outputCb); err != nil {
		return resp, http.StatusBadRequest, err
	}

	p.Start()

	// push events to the pipeline
	for i, event := range req.Events {
		p.In(pipeline.SourceID(h.requests.Inc()), "fake", pipeline.NewOffsets(int64(i+1), nil), event, true, nil)
	}

	// collect result
	var result []ProcessResult
loop:
	for {
		select {
		case <-ctx.Done():
			h.logger.Warn("request timed out") // e.g. some events were discarded
			break loop
		case event := <-events:
			result = append(result, ProcessResult{
				Event: event,
			})
			if len(result) >= len(req.Events) {
				break loop
			}
		}
	}
	p.Stop()

	// collect metrics
	metricsInfo, err := metricsRegistry.Gather()
	if err != nil {
		h.logger.Error("can't gather metrics", zap.Error(err))
	}

	_ = stdout.Sync()

	return DoActionsResponse{
		Result:  result,
		Stdout:  stdoutBuf.String(),
		Metrics: formatMetricFamily(metricsInfo),
	}, http.StatusOK, nil
}

func (h *DoActionsHandler) setupPipeline(p *pipeline.Pipeline, req DoActionsRequest, cb func(event *pipeline.Event)) error {
	if req.Actions == nil {
		req.Actions = []json.RawMessage{[]byte(`[]`)}
	}

	actionsArray, _ := jsoniterConfig.Marshal(req.Actions)
	actionsRaw, err := simplejson.NewJson(actionsArray)
	if err != nil {
		return fmt.Errorf("read actions: %w", err)
	}
	values := map[string]int{
		"capacity":   pipelineCapacity,
		"gomaxprocs": runtime.GOMAXPROCS(0),
	}
	if err := fd.SetupActions(p, h.plugins, actionsRaw, values); err != nil {
		return err
	}

	// setup input
	inputStaticInfo, err := h.plugins.Get(pipeline.PluginKindInput, "fake")
	if err != nil {
		return err
	}

	inputPlugin, config := inputStaticInfo.Factory()
	inputStaticInfo.Config = config

	p.SetInput(&pipeline.InputPluginInfo{
		PluginStaticInfo: inputStaticInfo,
		PluginRuntimeInfo: &pipeline.PluginRuntimeInfo{
			Plugin: inputPlugin,
		},
	})

	// setup output
	outputStaticInfo, err := h.plugins.Get(pipeline.PluginKindOutput, "devnull")
	if err != nil {
		return err
	}

	outputPlugin, config := outputStaticInfo.Factory()
	outputStaticInfo.Config = config

	outputPlugin.(*devnull.Plugin).SetOutFn(cb)

	p.SetOutput(&pipeline.OutputPluginInfo{
		PluginStaticInfo: outputStaticInfo,
		PluginRuntimeInfo: &pipeline.PluginRuntimeInfo{
			Plugin: outputPlugin,
		},
	})
	return nil
}

func (h *DoActionsHandler) unmarshalRequest(r *http.Request) (DoActionsRequest, error) {
	defer func(body io.ReadCloser) {
		_ = body.Close()
	}(r.Body)

	bodyRaw, err := io.ReadAll(r.Body)
	if err != nil {
		return DoActionsRequest{}, fmt.Errorf("reading body: %s", err)
	}

	if strings.HasSuffix(r.Header.Get("Content-Type"), "yaml") {
		bodyRaw, err = yaml.YAMLToJSON(bodyRaw)
		if err != nil {
			return DoActionsRequest{}, fmt.Errorf("converting YAML to JSON: %s", err)
		}
	}

	var req DoActionsRequest
	if err := jsoniterConfig.Unmarshal(bodyRaw, &req); err != nil {
		return DoActionsRequest{}, fmt.Errorf("unmarshalling json: %s", err)
	}
	return req, nil
}

func preparePipelineLogger(buf *bytes.Buffer) *zap.Logger {
	encoderConfig := zap.NewProductionEncoderConfig()
	encoderConfig.EncodeTime = zapcore.TimeEncoderOfLayout("05.000000")

	stdout := zap.New(
		zapcore.NewCore(
			zapcore.NewJSONEncoder(encoderConfig),
			zapcore.AddSync(buf),
			zapcore.DebugLevel,
		),
		zap.WithFatalHook(zapcore.WriteThenGoexit),
		zap.WithCaller(false),
		zap.WithClock(NewZeroClock(time.Now())),
	)
	return stdout
}

func formatMetricFamily(families []*dto.MetricFamily) string {
	b := new(bytes.Buffer)
	for _, f := range families {
		_ = expfmt.NewEncoder(b, expfmt.FmtOpenMetrics).Encode(f)
		b.WriteString("\n")
	}
	return b.String()
}

func jsonEscape(s string) string {
	b, err := jsoniterConfig.Marshal(s)
	if err != nil {
		panic(err)
	}
	// Trim the beginning and trailing " character
	return string(b[1 : len(b)-1])
}
