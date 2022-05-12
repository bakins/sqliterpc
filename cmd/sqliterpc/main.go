package main

import (
	"context"
	"log"
	"net/http"
	"os/signal"
	"syscall"

	"github.com/NYTimes/gziphandler"
	"github.com/alecthomas/kong"

	"github.com/bakins/sqliterpc"
	"github.com/bakins/sqliterpc/server"
)

func main() {
	// TODO logger stats, etc

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
	db, err := server.New(cfg.Database)
	if err != nil {
		return err
	}

	ts := sqliterpc.NewDatabaseServiceServer(db)

	mux := http.NewServeMux()
	mux.Handle(ts.PathPrefix(), ts)

	return http.ListenAndServe("127.0.0.1:8080", gziphandler.GzipHandler(mux))
}
