package router

import (
	"TTOQPR/gdp-tenant/pkg/health"
	"TTOQPR/gdp-tenant/pkg/tenant"

	"github.com/labstack/echo/v4"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type Router struct {
	server           *echo.Echo
	tenantController tenant.TenantServiceInterface
}

func NewRouter(server *echo.Echo, tenantController *tenant.TenantController) *Router {
	return &Router{
		server,
		tenantController,
	}
}

func (r *Router) Init() error {
	//create a default router with default middleware
	basePath := r.server.Group("/v1/gdp")

	basePath.GET("/health", health.HealthCheck)
	//tenant
	{
		basePath.POST("/tenant/create", r.tenantController.CreateTenant)
		// basePath.GET("/tenant/containermfs/:name", r.tenantController.GetContainerMFByTenantName)
		basePath.GET("/metrics", echo.WrapHandler(promhttp.Handler()))
	}
	return nil
}
