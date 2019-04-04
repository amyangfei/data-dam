package main

import (
	"flag"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/pingcap/errors"

	"github.com/amyangfei/data-dam/dam/central"
	_ "github.com/amyangfei/data-dam/db/mysql" // Register MySQL database
	"github.com/amyangfei/data-dam/pkg/log"
	"github.com/amyangfei/data-dam/pkg/utils"
)

func main() {
	cfg := central.NewConfig()
	err := cfg.Parse(os.Args[1:])
	switch errors.Cause(err) {
	case nil:
	case flag.ErrHelp:
		os.Exit(0)
	default:
		log.Errorf("parse cmd flags err %s", err)
		os.Exit(2)
	}

	log.SetLevelByString(strings.ToLower(cfg.LogLevel))
	if len(cfg.LogFile) > 0 {
		log.SetOutputByName(cfg.LogFile)
	}

	utils.PrintInfo("central-controller", func() {
		log.Infof("config: %s", cfg)
	})

	sc := make(chan os.Signal, 1)
	signal.Notify(sc,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)

	controller := central.NewController(cfg)

	go func() {
		sig := <-sc
		log.Infof("got signal [%v], exit", sig)
		controller.Close()
	}()

	err = controller.Start()
	if err != nil {
		log.Errorf("central controller starts with error %v", errors.ErrorStack(err))
	}
	controller.Close()

	log.Info("controller exit")
}
