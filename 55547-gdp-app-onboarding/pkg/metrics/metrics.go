package metrics

import (
	"TTOQPR/gdp-tenant/pkg/k8s"
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"k8s.io/klog"
)

var (
	gdp_api_onboard_tenant_status_guager = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "gdp_api_onboard_tenant_status",
			Help: "gdp api onboard tenant status",
		},
		[]string{
			"itam",
			"bc_rating",
			"tenant_name",
			"namespace_count",
		},
	)
	gdp_api_onboard_tenant_resource_request_guager = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "gdp_api_onboard_tenant_resource_request",
			Help: "gdp api onboard tenant resource request",
		},
		[]string{
			"resource",
			"itam",
			"tenant_name",
		},
	)
	gdp_api_onboard_tenant_resource_limit_guager = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "gdp_api_onboard_tenant_resource_limit",
			Help: "gdp api onboard tenant resource limit",
		},
		[]string{
			"resource",
			"itam",
			"tenant_name",
		},
	)
)

func init() {
	InitMetrics()
}

func InitMetrics() {
	prometheus.MustRegister(gdp_api_onboard_tenant_status_guager)
	prometheus.MustRegister(gdp_api_onboard_tenant_resource_request_guager)
	prometheus.MustRegister(gdp_api_onboard_tenant_resource_limit_guager)
}

func loadMetrics(cf *k8s.ClientFactory) {
	tenants, err := cf.GDPManager.ListTenants(context.Background())
	if err != nil {
		klog.Errorf("failed to list tenants: %v", err)
		return
	}
	for _, tenant := range tenants {
		gdp_api_onboard_tenant_status_guager.WithLabelValues(
			fmt.Sprintf("%v", tenant.Spec.Itam),
			fmt.Sprintf("%v", tenant.Spec.BC),
			fmt.Sprintf("%v", tenant.Name),
			fmt.Sprintf("%v", tenant.Spec.Namespace),
		).Set(1)
		requestCPU, _ := strconv.Atoi(tenant.Spec.ResourceQuotas.CPU)
		gdp_api_onboard_tenant_resource_request_guager.WithLabelValues(
			"cpu",
			fmt.Sprintf("%v", tenant.Spec.Itam),
			fmt.Sprintf("%v", tenant.Name),
		).Set(float64(requestCPU))

		requestMemory, _ := strconv.Atoi(tenant.Spec.ResourceQuotas.Memory)
		gdp_api_onboard_tenant_resource_request_guager.WithLabelValues(
			"memory",
			fmt.Sprintf("%v", tenant.Spec.Itam),
			fmt.Sprintf("%v", tenant.Name),
		).Set(float64(requestMemory))
	}

}
func SetIntervel(ctx context.Context, cf *k8s.ClientFactory) error {
	ticker := time.NewTicker(3 * time.Second)
	go func() {
		for {
			select {
			case <-ticker.C:
				loadMetrics(cf)
			case <-ctx.Done():
				ticker.Stop()
				return
			}
		}
	}()
	return nil
}
