package main

import (
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	corelog "log"

	"github.com/go-kit/kit/log"
	kitprometheus "github.com/go-kit/kit/metrics/prometheus"
	"github.com/microservices-demo/user/api"
	"github.com/microservices-demo/user/db"
	"github.com/microservices-demo/user/db/mongodb"
	opentracing "github.com/opentracing/opentracing-go"
	zipkinot "github.com/openzipkin-contrib/zipkin-go-opentracing"
	zgo "github.com/openzipkin/zipkin-go"
	zipkinhttp "github.com/openzipkin/zipkin-go/reporter/http"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	port string
	zip  string
)

// Define metrics with consistent labels
var (
	requestLatency = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "microservices_demo",
			Subsystem: "user",
			Name:      "request_latency_seconds",
			Help:      "Time (in seconds) spent serving HTTP requests.",
			Buckets:   prometheus.DefBuckets,
		},
		[]string{"method", "status_code"},
	)

	requestsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "microservices_demo",
			Subsystem: "user",
			Name:      "requests_total",
			Help:      "Total number of HTTP requests.",
		},
		[]string{"method", "status_code"},
	)
)

const (
	ServiceName = "user"
)

func init() {
	// Register metrics
	prometheus.MustRegister(requestLatency)
	prometheus.MustRegister(requestsTotal)
	
	flag.StringVar(&zip, "zipkin", os.Getenv("ZIPKIN"), "Zipkin address")
	flag.StringVar(&port, "port", "8084", "Port on which to run")
	db.Register("mongodb", &mongodb.Mongo{})
}

func main() {
	flag.Parse()
	
	// Mechanical stuff.
	errc := make(chan error)

	// Log domain.
	var logger log.Logger
	{
		logger = log.NewLogfmtLogger(os.Stderr)
		logger = log.With(logger, "ts", log.DefaultTimestampUTC)
		logger = log.With(logger, "caller", log.DefaultCaller)
	}

	// Find service local IP.
	conn, err := net.Dial("udp", "8.8.8.8:80")
	if err != nil {
		logger.Log("err", err)
		os.Exit(1)
	}
	localAddr := conn.LocalAddr().(*net.UDPAddr)
	host := strings.Split(localAddr.String(), ":")[0]
	defer conn.Close()

	var tracer opentracing.Tracer
	{
		if zip != "" {
			logger := log.With(logger, "tracer", "Zipkin")
			logger.Log("addr", zip)

			reporter := zipkinhttp.NewReporter(zip)
			defer reporter.Close()

			endpoint, err := zgo.NewEndpoint(ServiceName, fmt.Sprintf("%v:%v", host, port))
			if err != nil {
				logger.Log("err", "unable to create local endpoint", "error", err)
				os.Exit(1)
			}

			t, err := zgo.NewTracer(reporter, zgo.WithLocalEndpoint(endpoint))
			if err != nil {
				logger.Log("err", "unable to create tracer", "error", err)
				os.Exit(1)
			}

			tracer = zipkinot.Wrap(t)
			opentracing.SetGlobalTracer(tracer)
		} else {
			tracer = opentracing.NoopTracer{}
		}
	}

	// Database connection
	dbconn := false
	for !dbconn {
		err := db.Init()
		if err != nil {
			if err == db.ErrNoDatabaseSelected {
				corelog.Fatal(err)
			}
			corelog.Print(err)
			time.Sleep(time.Second)
		} else {
			dbconn = true
		}
	}

	// Service domain.
	fieldKeys := []string{"method"}
	var service api.Service
	{
		service = api.NewFixedService()
		service = api.LoggingMiddleware(logger)(service)
		service = api.NewInstrumentingService(
			kitprometheus.NewCounterFrom(
				prometheus.CounterOpts{
					Namespace: "microservices_demo",
					Subsystem: "user",
					Name:      "request_count",
					Help:      "Number of requests received.",
				},
				fieldKeys),
			kitprometheus.NewSummaryFrom(prometheus.SummaryOpts{
				Namespace: "microservices_demo",
				Subsystem: "user",
				Name:      "request_latency_microseconds",
				Help:      "Total duration of requests in microseconds.",
			}, fieldKeys),
			service,
		)
	}

	// Endpoint domain.
	endpoints := api.MakeEndpoints(service, tracer)

	// HTTP router
	router := api.MakeHTTPHandler(endpoints, logger, tracer)

	// Simple instrumentation middleware
	instrumentMiddleware := func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			
			// Call the next handler
			next.ServeHTTP(w, r)
			
			// Record the duration
			duration := time.Since(start).Seconds()
			
			// Update metrics with basic labels
			requestLatency.WithLabelValues(
				r.Method,
				"200", // This is simplified - you might want to capture actual status code
			).Observe(duration)
			
			requestsTotal.WithLabelValues(
				r.Method,
				"200",
			).Inc()
		})
	}

	// Create the server
	mux := http.NewServeMux()
	
	// Add metrics endpoint
	mux.Handle("/metrics", promhttp.Handler())
	
	// Add main application endpoints with instrumentation
	mux.Handle("/", instrumentMiddleware(router))

	// HTTP Server
	server := &http.Server{
		Addr:    fmt.Sprintf(":%v", port),
		Handler: mux,
	}

	// Start server
	go func() {
		logger.Log("transport", "HTTP", "addr", port)
		errc <- server.ListenAndServe()
	}()

	// Capture interrupts
	go func() {
		c := make(chan os.Signal, 1)
		signal.Notify(c, syscall.SIGINT, syscall.SIGTERM)
		errc <- fmt.Errorf("%s", <-c)
	}()

	logger.Log("exit", <-errc)
}
