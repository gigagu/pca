package router

import (
	"testing"
	"TTOQPR/gdp-tenant/pkg/k8s"
	"TTOQPR/gdp-tenant/pkg/tenant"

	"github.com/labstack/echo/v4"
	"github.com/stretchr/testify/assert"
)

type mockTenantController struct {
	tenant.TenantServiceInterface
}

func TestNewRouter(t *testing.T) {
	e := echo.New()

	cf, _ := k8s.NewClientFactory()
	tc, _ := tenant.NewTenantController(cf)
	router := NewRouter(e, tc)

	assert.NotNil(t, router)
	assert.Equal(t, e, router.server)
}
func TestRouter_Init(t *testing.T) {
	e := echo.New()
	cf, _ := k8s.NewClientFactory()
	tc, _ := tenant.NewTenantController(cf)
	router := NewRouter(e, tc)

	err := router.Init()
	assert.NoError(t, err)

	// Check that the routes are registered
	routes := e.Routes()
	var healthFound, tenantCreateFound, metricsFound bool

	for _, route := range routes {
		switch {
		case route.Path == "/v1/gdp/health" && route.Method == echo.GET:
			healthFound = true
		case route.Path == "/v1/gdp/tenant/create" && route.Method == echo.POST:
			tenantCreateFound = true
		case route.Path == "/v1/gdp/metrics" && route.Method == echo.GET:
			metricsFound = true
		}
	}

	assert.True(t, healthFound, "Health route should be registered")
	assert.True(t, tenantCreateFound, "Tenant create route should be registered")
	assert.True(t, metricsFound, "Metrics route should be registered")
}
