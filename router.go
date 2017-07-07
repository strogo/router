package main

import (
	"fmt"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/alphagov/router/handlers"
	"github.com/alphagov/router/logger"
	"github.com/alphagov/router/triemux"
	"gopkg.in/mgo.v2"
  "github.com/aws/aws-sdk-go/service/dynamodb"
  "github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
  "github.com/aws/aws-sdk-go/aws/session"
  "github.com/aws/aws-sdk-go/aws"
)

// Router is a wrapper around an HTTP multiplexer (trie.Mux) which retrieves its
// routes from a passed mongo database.
type Router struct {
	mux                   *triemux.Mux
	lock                  sync.RWMutex
	mongoURL              string
	mongoDbName           string
	backendConnectTimeout time.Duration
	backendHeaderTimeout  time.Duration
	logger                logger.Logger
}

type Backend struct {
	BackendID  string `dynamodbav:"backend_id"`
	BackendURL string `dynamodbav:"backend_url"`
}

type Route struct {
	IncomingPath string `dynamodbav:"incoming_path"`
	RouteType    string `dynamodbav:"route_type"`
	Handler      string `dynamodbav:"handler"`
	BackendID    string `dynamodbav:"backend_id"`
	RedirectTo   string `dynamodbav:"redirect_to"`
	RedirectType string `dynamodbav:"redirect_type"`
	SegmentsMode string `dynamodbav:"segments_mode"`
	Disabled     bool   `dynamodbav:"disabled"`
}

// NewRouter returns a new empty router instance. You will still need to call
// ReloadRoutes() to do the initial route load.
func NewRouter(mongoURL, mongoDbName, backendConnectTimeout, backendHeaderTimeout, logFileName string) (rt *Router, err error) {
	beConnTimeout, err := time.ParseDuration(backendConnectTimeout)
	if err != nil {
		return nil, err
	}
	beHeaderTimeout, err := time.ParseDuration(backendHeaderTimeout)
	if err != nil {
		return nil, err
	}
	logInfo("router: using backend connect timeout:", beConnTimeout)
	logInfo("router: using backend header timeout:", beHeaderTimeout)

	l, err := logger.New(logFileName)
	if err != nil {
		return nil, err
	}
	logInfo("router: logging errors as JSON to", logFileName)

	rt = &Router{
		mux:                   triemux.NewMux(),
		mongoURL:              mongoURL,
		mongoDbName:           mongoDbName,
		backendConnectTimeout: beConnTimeout,
		backendHeaderTimeout:  beHeaderTimeout,
		logger:                l,
	}
	return rt, nil
}

// ServeHTTP delegates responsibility for serving requests to the proxy mux
// instance for this router.
func (rt *Router) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	defer func() {
		if r := recover(); r != nil {
			logWarn("router: recovered from panic in ServeHTTP:", r)
			rt.logger.LogFromClientRequest(map[string]interface{}{"error": fmt.Sprintf("panic: %v", r), "status": 500}, req)
			w.WriteHeader(http.StatusInternalServerError)
		}
	}()
	rt.lock.RLock()
	mux := rt.mux
	rt.lock.RUnlock()

	mux.ServeHTTP(w, req)
}

// ReloadRoutes reloads the routes for this Router instance on the fly. It will
// create a new proxy mux, load applications (backends) and routes into it, and
// then flip the "mux" pointer in the Router.
func (rt *Router) ReloadRoutes() {
	defer func() {
		if r := recover(); r != nil {
			logWarn("router: recovered from panic in ReloadRoutes:", r)
			logInfo("router: original routes have not been modified")
		}
	}()

	logDebug("mgo: connecting to", rt.mongoURL)
	sess, err := mgo.Dial(rt.mongoURL)
	if err != nil {
		panic(fmt.Sprintln("mgo:", err))
	}
	defer sess.Close()
	sess.SetMode(mgo.Strong, true)

  ddb := dynamodb.New(session.New())
  backendsQuery := &dynamodb.ScanInput{
    TableName:            aws.String("backends"),
  }

  result, err := ddb.Scan(backendsQuery)

	logInfo("router: reloading routes")
	newmux := triemux.NewMux()

	backends := rt.loadBackends(result)

	routesQuery := &dynamodb.ScanInput{
		TableName: aws.String("routes"),
	}

	err = ddb.ScanPages(routesQuery,
		func(page *dynamodb.ScanOutput, _ bool) bool {
				fmt.Println("Got page with", *page.Count)
				loadRoutes(page, newmux, backends)
				return true
		})

	fmt.Println(err)

	rt.lock.Lock()
	rt.mux = newmux
	rt.lock.Unlock()

	logInfo(fmt.Sprintf("router: reloaded %d routes (checksum: %x)", rt.mux.RouteCount(), rt.mux.RouteChecksum()))
}

// loadBackends is a helper function which loads backends from the
// passed mongo collection, constructs a Handler for each one, and returns
// them in map keyed on the backend_id
func (rt *Router) loadBackends(r *dynamodb.ScanOutput) (backends map[string]http.Handler) {
	backends_list := []Backend{}
	backends = make(map[string]http.Handler)

	dynamodbattribute.UnmarshalListOfMaps(r.Items, &backends_list)
	for _, b := range backends_list {
		fmt.Println(b)
		backendURL, err := url.Parse(b.BackendURL)
		if err != nil {
			logWarn(fmt.Sprintf("router: couldn't parse URL %s for backend %s "+
				"(error: %v), skipping!", b.BackendURL, b.BackendID, err))
			continue
		}

		backends[b.BackendID] = handlers.NewBackendHandler(backendURL, rt.backendConnectTimeout, rt.backendHeaderTimeout, rt.logger)
	}

	return
}

// loadRoutes is a helper function which loads routes from the passed mongo
// collection and registers them with the passed proxy mux.
func loadRoutes(p *dynamodb.ScanOutput, mux *triemux.Mux, backends map[string]http.Handler) {
	goneHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "410 gone", http.StatusGone)
	})
	unavailableHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "503 Service Unavailable", http.StatusServiceUnavailable)
	})

	routes := []Route{}

	dynamodbattribute.UnmarshalListOfMaps(p.Items, &routes)
	for _, route := range routes {
		prefix := (route.RouteType == "prefix")

		// the database contains paths with % encoded routes.
		// Unescape them here because the http.Request objects we match against contain the unescaped variants.
		incomingURL, err := url.Parse(route.IncomingPath)
		if err != nil {
			logWarn(fmt.Sprintf("router: found route %+v with invalid incoming path '%s', skipping!", route, route.IncomingPath))
			continue
		}

		if route.Disabled {
			mux.Handle(incomingURL.Path, prefix, unavailableHandler)
			logDebug(fmt.Sprintf("router: registered %s (prefix: %v)(disabled) -> Unavailable", incomingURL.Path, prefix))
			continue
		}

		switch route.Handler {
		case "backend":
			handler, ok := backends[route.BackendID]
			if !ok {
				logWarn(fmt.Sprintf("router: found route %+v which references unknown backend "+
					"%s, skipping!", route, route.BackendID))
				continue
			}
			mux.Handle(incomingURL.Path, prefix, handler)
			logDebug(fmt.Sprintf("router: registered %s (prefix: %v) for %s",
				incomingURL.Path, prefix, route.BackendID))
		case "redirect":
			redirectTemporarily := (route.RedirectType == "temporary")
			handler := handlers.NewRedirectHandler(incomingURL.Path, route.RedirectTo, shouldPreserveSegments(&route), redirectTemporarily)
			mux.Handle(incomingURL.Path, prefix, handler)
			logDebug(fmt.Sprintf("router: registered %s (prefix: %v) -> %s",
				incomingURL.Path, prefix, route.RedirectTo))
		case "gone":
			mux.Handle(incomingURL.Path, prefix, goneHandler)
			logDebug(fmt.Sprintf("router: registered %s (prefix: %v) -> Gone", incomingURL.Path, prefix))
		case "boom":
			// Special handler so that we can test failure behaviour.
			mux.Handle(incomingURL.Path, prefix, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				panic("Boom!!!")
			}))
			logDebug(fmt.Sprintf("router: registered %s (prefix: %v) -> Boom!!!", incomingURL.Path, prefix))
		default:
			logWarn(fmt.Sprintf("router: found route %+v with unknown handler type "+
				"%s, skipping!", route, route.Handler))
			continue
		}
	}
}

func (rt *Router) RouteStats() (stats map[string]interface{}) {
	rt.lock.RLock()
	mux := rt.mux
	rt.lock.RUnlock()

	stats = make(map[string]interface{})
	stats["count"] = mux.RouteCount()
	stats["checksum"] = fmt.Sprintf("%x", mux.RouteChecksum())
	return
}

func shouldPreserveSegments(route *Route) bool {
	switch {
	case route.RouteType == "exact" && route.SegmentsMode == "preserve":
		return true
	case route.RouteType == "exact":
		return false
	case route.RouteType == "prefix" && route.SegmentsMode == "ignore":
		return false
	case route.RouteType == "prefix":
		return true
	}
	return false
}
