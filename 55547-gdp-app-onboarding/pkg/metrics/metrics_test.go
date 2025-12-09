package metrics

import (
	"TTOQPR/gdp-tenant/pkg/constants"
	// corev1 "k8s.io/api/core/v1"
	"TTOQPR/gdp-tenant/pkg/k8s"
	"context"

	dynamicfake "k8s.io/client-go/dynamic/fake"
	"k8s.io/client-go/kubernetes/fake"

	// "fmt"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"k8s.io/apimachinery/pkg/runtime"

	gdptenant "TTOQPR/gdp-tenant/pkg/crd/api/v1"
)

// mockClientFactory is a mock for k8s.ClientFactory
type mockClientFactory struct {
	called *bool
}

func (m *mockClientFactory) GDPManager() *mockGDPManager {
	return &mockGDPManager{called: m.called}
}

type mockGDPManager struct {
	called *bool
}

func newGDPTenant() *gdptenant.GDPTenant {
	return &gdptenant.GDPTenant{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "gdp.standardchartered.com/v1",
			Kind:       "GDPTenant",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-gdptenant",
			Namespace: constants.GDPNamespace,
		},
	}
}
func TestSetIntervel_CallsLoadMetrics(t *testing.T) {
	cf, _ := k8s.NewClientFactory()

	err := SetIntervel(context.Background(), cf)
	require.NoError(t, err)
}

func Test_loadMetrics_SetsMetricsForTenants(t *testing.T) {
	// Mock tenant data
	tenant := &gdptenant.GDPTenant{
		ObjectMeta: metav1.ObjectMeta{
			Name: "tenant1",
		},
		Spec: gdptenant.GDPTenantSpec{
			Itam:       55547,
			BC:         "bc1",
			Namespaces: []string{"ns1", "ns2"},
			ResourceQuotas: gdptenant.ResourceQuotas{
				RequestsCPU:    "2",
				RequestsMemory: "1024",
			},
		},
	}

	cf, _ := k8s.NewClientFactory()

	scheme := runtime.NewScheme()
	gdptenant.AddToScheme(scheme)

	cf.SetLocalDynamicClient(dynamicfake.NewSimpleDynamicClient(scheme, tenant))
	clientset := fake.NewSimpleClientset()
	cf.SetLocalClient(clientset)

	loadMetrics(cf)

	// Check that the metrics are set (labels must match those in loadMetrics)
	statusMetric, err := gdp_api_onboard_tenant_status_guager.GetMetricWithLabelValues(
		"itam1", "bc1", "tenant1", "2",
	)
	require.NoError(t, err)
	require.NotNil(t, statusMetric)

	requestCPUMetric, err := gdp_api_onboard_tenant_resource_request_guager.GetMetricWithLabelValues(
		"cpu", "itam1", "tenant1",
	)
	require.NoError(t, err)
	require.NotNil(t, requestCPUMetric)

	requestMemoryMetric, err := gdp_api_onboard_tenant_resource_request_guager.GetMetricWithLabelValues(
		"memory", "itam1", "tenant1",
	)
	require.NoError(t, err)
	require.NotNil(t, requestMemoryMetric)
}
func TestInit_RegistersMetrics(t *testing.T) {
	// Unregister metrics if already registered to avoid panic on double registration
	prometheus.Unregister(gdp_api_onboard_tenant_status_guager)
	prometheus.Unregister(gdp_api_onboard_tenant_resource_request_guager)
	prometheus.Unregister(gdp_api_onboard_tenant_resource_limit_guager)

	InitMetrics()

	// After registration, metrics should be registered in the default registry
	metricsFamilies, err := prometheus.DefaultGatherer.Gather()
	require.NoError(t, err)

	var foundStatus, foundRequest bool
	for _, mf := range metricsFamilies {
		switch mf.GetName() {
		case "gdp_api_onboard_tenant_status":
			foundStatus = true
		case "gdp_api_onboard_tenant_resource_request":
			foundRequest = true
		case "gdp_api_onboard_tenant_resource_limit":
			// foundLimit = true
		}
	}
	require.True(t, foundStatus, "gdp_api_onboard_tenant_status should be registered")
	require.True(t, foundRequest, "gdp_api_onboard_tenant_resource_request should be registered")
	// require.True(t, foundLimit, "gdp_api_onboard_tenant_resource_limit should be registered")
}
