package main

import (
	"context"
	"log"
	"net/http"
	"os/signal"
	"syscall"

	"github.com/NYTimes/gziphandler"
	"github.com/alecthomas/kong"
	"github.com/justinas/alice"
	"github.com/twitchtv/twirp"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.uber.org/zap"

	"github.com/bakins/sqliterpc"
	"github.com/bakins/sqliterpc/internal/logging"
	"github.com/bakins/sqliterpc/server"
	"github.com/bakins/twirpotel"
)

func main() {
	var cli config

	kong.Parse(&cli)

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGQUIT)
	defer cancel()

	if err := run(ctx, cli); err != nil {
		log.Fatal(err)
	}
}

type config struct {
	Database string `kong:"default=sqliterpc.db"`
}

func run(ctx context.Context, cfg config) error {
	logger, err := zap.NewProduction()
	if err != nil {
		return err
	}

	defer logger.Sync()

	db, err := server.New(cfg.Database)
	if err != nil {
		return err
	}

	defer db.Close()

	chain := alice.New(
		logging.Middleware(logger),
		func(next http.Handler) http.Handler {
			return otelhttp.NewHandler(next, "sqliterpc")
		},
		gziphandler.GzipHandler,
	)

	ts := sqliterpc.NewDatabaseServiceServer(
		db,
		twirp.WithServerInterceptors(twirpotel.ServerInterceptor()),
	)

	mux := http.NewServeMux()
	mux.Handle(ts.PathPrefix(), ts)

	return http.ListenAndServe("127.0.0.1:8080", chain.Then(mux))
}
