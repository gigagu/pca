package k8s

import (
	gdptenant "TTOQPR/gdp-tenant/pkg/crd/api/v1"
	"encoding/json"

	"context"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

type GDPManager struct {
	cf *ClientFactory
}

func NewGDPManager(cf *ClientFactory) *GDPManager {
	return &GDPManager{
		cf: cf,
	}
}

type GDPTenantInterface interface {
	GetTenant(ctx context.Context, name string) (*gdptenant.GDPTenant, error)
	ApplyTenant(ctx context.Context, tenant *gdptenant.GDPTenant) (*gdptenant.GDPTenant, error)
	ListTenants(ctx context.Context) ([]gdptenant.GDPTenant, error)
}

func (m *GDPManager) ApplyTenant(ctx context.Context, tenant *gdptenant.GDPTenant) error {
	tenantSchema := schema.GroupVersionResource{Group: "gdp.standardchartered.com", Version: "v1", Resource: "gdptenants"}
	unstructuredObj, err := runtime.DefaultUnstructuredConverter.ToUnstructured(tenant)
	if err != nil {
		return err
	}
	var tenantUnstructured unstructured.Unstructured
	tenantUnstructured.Object = unstructuredObj

	_, err = m.cf.localDynamicClient.Resource(tenantSchema).Apply(ctx, tenant.Name, &tenantUnstructured, metav1.ApplyOptions{
		// _, err = m.cf.dynamicClient.Resource(tenantSchema).Namespace(constants.GDPNamespace).Apply(ctx, tenant.Name, &tenantUnstructured, metav1.ApplyOptions{
		FieldManager: "gdp-tenant",
		Force:        true,
	})
	return err
}

func (m *GDPManager) GetTenant(ctx context.Context, name string) (*gdptenant.GDPTenant, error) {
	tenantSchema := schema.GroupVersionResource{Group: "gdp.standardchartered.com", Version: "v1", Resource: "gdptenants"}
	unstructuredObj, err := m.cf.localDynamicClient.
		Resource(tenantSchema).
		Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}
	var tenant gdptenant.GDPTenant
	err = runtime.DefaultUnstructuredConverter.FromUnstructured(unstructuredObj.Object, &tenant)
	if err != nil {
		return nil, err
	}
	return &tenant, nil

}

func (m *GDPManager) ListTenants(ctx context.Context) ([]gdptenant.GDPTenant, error) {
	tenantSchema := schema.GroupVersionResource{Group: "gdp.standardchartered.com", Version: "v1", Resource: "gdptenants"}

	tenantList, err := m.cf.localDynamicClient.Resource(tenantSchema).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	var tenants []gdptenant.GDPTenant
	for _, item := range tenantList.Items {
		var tenant gdptenant.GDPTenant
		err := runtime.DefaultUnstructuredConverter.FromUnstructured(item.Object, &tenant)
		if err != nil {
			return nil, err
		}
		tenants = append(tenants, tenant)
	}
	return tenants, nil
}

func (m *GDPManager) PatchTenantStatus(ctx context.Context, name string, status gdptenant.GDPTenantStatus) error {
	tenantSchema := schema.GroupVersionResource{Group: "gdp.standardchartered.com", Version: "v1", Resource: "gdptenants"}

	patch := map[string]interface{}{
		"status": status,
	}
	patchBytes, err := json.Marshal(patch)
	if err != nil {
		return err
	}

	_, err = m.cf.localDynamicClient.Resource(tenantSchema).
		Patch(ctx, name, types.MergePatchType, patchBytes, metav1.PatchOptions{}, "status")
	return err
}
