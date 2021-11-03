package telemetry

import (
	"time"

	"github.com/ankur-anand/pubhub/hub"
	pbhub "github.com/ankur-anand/pubhub/proto/gen/v1/hub"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"google.golang.org/protobuf/proto"
)

// prometheus metrics exporter
var (
	streamCounterMetric = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "pubhub_connected_stream_count",
		Help: "number of stream connected.",
	}, []string{"kind"})

	publishDurationMetrics = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name: "pubhub_publish_stream_duration_seconds",
		Help: "Duration of time it took for publish on stream",
	}, []string{"success"})

	publishCounter = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "pubhub_publish_stream_send_total",
		Help: "Total number of publish event processed.",
	}, []string{"success"})

	streamDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "pubhub_established_stream_duration_seconds",
		Help:    "Duration of time a stream remains connected with hubserver.",
		Buckets: []float64{10, 60, 300, 3600, 86400, 8640000, 17280000, 25920000}, // buckets are in days, mostly used for averaging out.
	}, []string{"kind"})

	sizeMsgCounter = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "pubhub_message_published_bytes",
		Help: "total bytes send to client stream",
	}, []string{})
)

// PromHook implements hub.Hooker for Prometheus metrics export format.
type PromHook struct {
}

func (p PromHook) Duration(metadata hub.HookMetadata, d time.Duration) {
	if metadata.DurationKind == hub.Broadcast {
		lv := "true"
		if metadata.Err != nil {
			lv = "false"
		}
		publishDurationMetrics.WithLabelValues(lv).Observe(d.Seconds())
		publishCounter.WithLabelValues(lv).Add(1)
		return
	}
	streamDuration.WithLabelValues("subscription").Observe(d.Seconds())
}

func (p PromHook) PubHook(msg *pbhub.KV) {
	sizeMsgCounter.WithLabelValues().Add(float64(proto.Size(msg)))
	return
}

func (p PromHook) SubHook(ops hub.SubscriberOPS) {
	if ops == hub.SubscriberCreate {
		streamCounterMetric.WithLabelValues("subscription").Add(1)
	}

	if ops == hub.SubscriberDelete {
		streamCounterMetric.WithLabelValues("subscription").Sub(1)
	}
}
