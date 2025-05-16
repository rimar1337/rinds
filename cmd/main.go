package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"time"

	_ "github.com/lib/pq"

	auth "github.com/ericvolp12/go-bsky-feed-generator/pkg/auth"
	"github.com/ericvolp12/go-bsky-feed-generator/pkg/feedrouter"
	ginendpoints "github.com/ericvolp12/go-bsky-feed-generator/pkg/gin"

	freshfeeds "github.com/ericvolp12/go-bsky-feed-generator/pkg/feeds/fresh"
	staticfeed "github.com/ericvolp12/go-bsky-feed-generator/pkg/feeds/static"
	ginprometheus "github.com/ericvolp12/go-gin-prometheus"
	"github.com/gin-gonic/gin"
	"go.opentelemetry.io/contrib/instrumentation/github.com/gin-gonic/gin/otelgin"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.17.0"
)

func main() {
	ctx := context.Background()

	// Open the database connection
	dbHost := os.Getenv("DB_HOST")
	dbUser := os.Getenv("DB_USER")
	dbName := os.Getenv("DB_NAME")
	dbPassword := os.Getenv("DB_PASSWORD")
	db, err := sql.Open("postgres", fmt.Sprintf("user=%s dbname=%s host=%s password=%s sslmode=disable", dbUser, dbName, dbHost, dbPassword))
	if err != nil {
		log.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Ping the database to ensure the connection is established
	if err := db.Ping(); err != nil {
		log.Fatalf("Failed to ping database: %v", err)
	}

	// Configure feed generator from environment variables

	// Registers a tracer Provider globally if the exporter endpoint is set
	if os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT") != "" {
		log.Println("initializing tracer...")
		shutdown, err := installExportPipeline(ctx)
		if err != nil {
			log.Fatal(err)
		}
		defer func() {
			if err := shutdown(ctx); err != nil {
				log.Fatal(err)
			}
		}()
	}

	feedActorDID := os.Getenv("FEED_ACTOR_DID")
	if feedActorDID == "" {
		log.Fatal("FEED_ACTOR_DID environment variable must be set")
	}

	// serviceEndpoint is a URL that the feed generator will be available at
	serviceEndpoint := os.Getenv("SERVICE_ENDPOINT")
	if serviceEndpoint == "" {
		log.Fatal("SERVICE_ENDPOINT environment variable must be set")
	}

	// Set the acceptable DIDs for the feed generator to respond to
	// We'll default to the feedActorDID and the Service Endpoint as a did:web
	serviceURL, err := url.Parse(serviceEndpoint)
	if err != nil {
		log.Fatal(fmt.Errorf("error parsing service endpoint: %w", err))
	}

	serviceWebDID := "did:web:" + serviceURL.Hostname()

	log.Printf("service DID Web: %s", serviceWebDID)

	acceptableDIDs := []string{feedActorDID, serviceWebDID}

	// Create a new feed router instance
	feedRouter, err := feedrouter.NewFeedRouter(ctx, feedActorDID, serviceWebDID, acceptableDIDs, serviceEndpoint)
	if err != nil {
		log.Fatal(fmt.Errorf("error creating feed router: %w", err))
	}

	// Here we can add feeds to the Feed Router instance
	// Feeds conform to the Feed interface, which is defined in
	// pkg/feedrouter/feedrouter.go

	// For demonstration purposes, we'll use a static feed generator
	// that will always return the same feed skeleton (one post)
	staticFeed, staticFeedAliases, err := staticfeed.NewStaticFeed(
		ctx,
		feedActorDID,
		"static",
		// This static post is the conversation that sparked this demo repo
		[]string{"at://did:plc:q6gjnaw2blty4crticxkmujt/app.bsky.feed.post/3jx7msc4ive26"},
	)

	// idk help me

	rindsFeed, rindsFeedAliases, err := freshfeeds.NewStaticFeed(
		ctx,
		feedActorDID,
		"rinds",
		// This static post is the conversation that sparked this demo repo
		[]string{"at://did:plc:mn45tewwnse5btfftvd3powc/app.bsky.feed.post/3kgjjhlsnoi2f"},
		db,
		"rinds",
		false,
	)

	localrindsFeed, localrindsFeedAliases, err := freshfeeds.NewStaticFeed(
		ctx,
		feedActorDID,
		"localrinds-test",
		// This static post is the conversation that sparked this demo repo
		[]string{"at://did:plc:mn45tewwnse5btfftvd3powc/app.bsky.feed.post/3kgjjhlsnoi2f"},
		db,
		"localrinds-test",
		false,
	)

	randomFeed, randomFeedAliases, err := freshfeeds.NewStaticFeed(
		ctx,
		feedActorDID,
		"random",
		// This static post is the conversation that sparked this demo repo
		[]string{"at://did:plc:mn45tewwnse5btfftvd3powc/app.bsky.feed.post/3kgjjhlsnoi2f"},
		db,
		"random",
		false,
	)

	repostsFeed, repostsFeedAliases, err := freshfeeds.NewStaticFeed(
		ctx,
		feedActorDID,
		"reposts",
		// This static post is the conversation that sparked this demo repo
		[]string{"at://did:plc:mn45tewwnse5btfftvd3powc/app.bsky.feed.post/3kgjjhlsnoi2f"},
		db,
		"reposts",
		false,
	)
	mnineFeed, mnineFeedAliases, err := freshfeeds.NewStaticFeed(
		ctx,
		feedActorDID,
		"mnine",
		// This static post is the conversation that sparked this demo repo
		[]string{"at://did:plc:mn45tewwnse5btfftvd3powc/app.bsky.feed.post/3kgjjhlsnoi2f"},
		db,
		"mnine",
		false,
	)

	rrindsFeed, rrindsFeedAliases, err := freshfeeds.NewStaticFeed(
		ctx,
		feedActorDID,
		"rinds-replies",
		// This static post is the conversation that sparked this demo repo
		[]string{"at://did:plc:mn45tewwnse5btfftvd3powc/app.bsky.feed.post/3kgjjhlsnoi2f"},
		db,
		"rinds",
		true,
	)

	rrandomFeed, rrandomFeedAliases, err := freshfeeds.NewStaticFeed(
		ctx,
		feedActorDID,
		"random-replies",
		// This static post is the conversation that sparked this demo repo
		[]string{"at://did:plc:mn45tewwnse5btfftvd3powc/app.bsky.feed.post/3kgjjhlsnoi2f"},
		db,
		"random",
		true,
	)

	rrepostsFeed, rrepostsFeedAliases, err := freshfeeds.NewStaticFeed(
		ctx,
		feedActorDID,
		"reposts-replies",
		// This static post is the conversation that sparked this demo repo
		[]string{"at://did:plc:mn45tewwnse5btfftvd3powc/app.bsky.feed.post/3kgjjhlsnoi2f"},
		db,
		"reposts",
		true,
	)
	rmnineFeed, rmnineFeedAliases, err := freshfeeds.NewStaticFeed(
		ctx,
		feedActorDID,
		"mnine-replies",
		// This static post is the conversation that sparked this demo repo
		[]string{"at://did:plc:mn45tewwnse5btfftvd3powc/app.bsky.feed.post/3kgjjhlsnoi2f"},
		db,
		"mnine",
		true,
	)
	orepliesFeed, orepliesFeedAliases, err := freshfeeds.NewStaticFeed(
		ctx,
		feedActorDID,
		"oreplies",
		// This static post is the conversation that sparked this demo repo
		[]string{"at://did:plc:mn45tewwnse5btfftvd3powc/app.bsky.feed.post/3kgjjhlsnoi2f"},
		db,
		"oreplies",
		true,
	)
	// Add the static feed to the feed generator
	feedRouter.AddFeed(staticFeedAliases, staticFeed)

	feedRouter.AddFeed(localrindsFeedAliases, localrindsFeed)

	feedRouter.AddFeed(rindsFeedAliases, rindsFeed)
	feedRouter.AddFeed(randomFeedAliases, randomFeed)
	feedRouter.AddFeed(repostsFeedAliases, repostsFeed)
	feedRouter.AddFeed(mnineFeedAliases, mnineFeed)

	feedRouter.AddFeed(rrindsFeedAliases, rrindsFeed)
	feedRouter.AddFeed(rrandomFeedAliases, rrandomFeed)
	feedRouter.AddFeed(rrepostsFeedAliases, rrepostsFeed)
	feedRouter.AddFeed(rmnineFeedAliases, rmnineFeed)

	feedRouter.AddFeed(orepliesFeedAliases, orepliesFeed)

	// Create a gin router with default middleware for logging and recovery
	router := gin.Default()

	// Plug in OTEL Middleware and skip metrics endpoint
	router.Use(
		otelgin.Middleware(
			"go-bsky-feed-generator",
			otelgin.WithFilter(func(req *http.Request) bool {
				return req.URL.Path != "/metrics"
			}),
		),
	)

	// Add Prometheus metrics middleware
	p := ginprometheus.NewPrometheus("gin", nil)
	p.Use(router)

	// Add unauthenticated routes for feed generator
	ep := ginendpoints.NewEndpoints(feedRouter)
	router.GET("/.well-known/did.json", ep.GetWellKnownDID)
	router.GET("/xrpc/app.bsky.feed.describeFeedGenerator", ep.DescribeFeeds)
	// Root route: ASCII art and GitHub link
	router.GET("/", func(c *gin.Context) {
		c.Header("Content-Type", "text/plain; charset=utf-8")
		c.String(http.StatusOK, `   ____  _           _     
  |  _ \(_)_ __   __| |___ 
  | |_) | | '_ \ / _' / __|
  |  _ <| | | | | (_| \__ \
  |_| \_\_|_| |_|\__,_|___/
                            
bsky feed generators by @whey.party

Code: https://github.com/rimar1337/rinds
Flagship Fresh Feeds Instance: https://bsky.app/profile/did:plc:mn45tewwnse5btfftvd3powc/feed/rinds

Fresh Feeds icons by @pprmint.de
Repository generated from https://github.com/ericvolp12/go-bsky-feed-generator
`)
	})

	// Plug in Authentication Middleware
	auther, err := auth.NewAuth(
		100_000,
		time.Hour*12,
		5,
		serviceWebDID,
	)
	if err != nil {
		log.Fatalf("Failed to create Auth: %v", err)
	}

	router.Use(auther.AuthenticateGinRequestViaJWT)

	// Add authenticated routes for feed generator
	router.GET("/xrpc/app.bsky.feed.getFeedSkeleton", ep.GetFeedSkeleton)

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	log.Printf("Starting server on port %s", port)
	router.Run(fmt.Sprintf(":%s", port))
}

// installExportPipeline registers a trace provider instance as a global trace provider,
func installExportPipeline(ctx context.Context) (func(context.Context) error, error) {
	client := otlptracehttp.NewClient()
	exporter, err := otlptrace.New(ctx, client)
	if err != nil {
		return nil, fmt.Errorf("creating OTLP trace exporter: %w", err)
	}

	tracerProvider := newTraceProvider(exporter)
	otel.SetTracerProvider(tracerProvider)

	return tracerProvider.Shutdown, nil
}

// newTraceProvider creates a new trace provider instance.
func newTraceProvider(exp sdktrace.SpanExporter) *sdktrace.TracerProvider {
	// Ensure default SDK resources and the required service name are set.
	r, err := resource.Merge(
		resource.Default(),
		resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceName("go-bsky-feed-generator"),
		),
	)

	if err != nil {
		panic(err)
	}

	// initialize the traceIDRatioBasedSampler to sample all traces
	traceIDRatioBasedSampler := sdktrace.TraceIDRatioBased(1)

	return sdktrace.NewTracerProvider(
		sdktrace.WithSampler(traceIDRatioBasedSampler),
		sdktrace.WithBatcher(exp),
		sdktrace.WithResource(r),
	)
}
