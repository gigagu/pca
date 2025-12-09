package main

import (
	"context"
	"flag"
	"os"
	"os/signal"
	"syscall"
	"time"

	"TTOQPR/gdp-tenant/pkg/k8s"
	"TTOQPR/gdp-tenant/pkg/metrics"
	"TTOQPR/gdp-tenant/pkg/router"
	"TTOQPR/gdp-tenant/pkg/tenant"

	"github.com/labstack/echo/v4"
	"k8s.io/klog/v2"
)

var (
	kubeconfig string
	configPath string
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	flag.Parse()
	klog.InitFlags(nil)

	// start http server
	e := echo.New()
	// e.Use(middlewares.LoggingMiddleware)

	e.Debug = true

	// e.GET("/health", func(c echo.Context) error {
	// 	return c.String(200, "ok")
	// })

	cf, err := k8s.NewClientFactory()
	if err != nil {
		klog.Fatalf("failed to create k8s client factory: %v", err)
	}
	cf.SetMinioCredential()

	tc, err := tenant.NewTenantController(cf)
	if err != nil {
		klog.Fatalf("failed to create tenant controller: %v", err)
	}

	r := router.NewRouter(e, tc)
	r.Init()
	// e.POST("/tenant", createTenantHandler)

	go func() {
		if err := e.Start(":6443"); err != nil {
			e.Logger.Info("shutting down the server")
		}
	}()

	metrics.SetIntervel(ctx, cf)

	<-ctx.Done()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := e.Shutdown(ctx); err != nil {
		e.Logger.Fatal(err)
	}
	klog.Flush()
	time.Sleep(1 * time.Second)

}
