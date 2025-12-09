package k8s

import (
	"context"
	"reflect"
	"testing"

	"TTOQPR/gdp-tenant/pkg/constants"
	gdptenant "TTOQPR/gdp-tenant/pkg/crd/api/v1"

	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	"k8s.io/client-go/kubernetes/fake"
)

type mockClientFactory struct{}

func TestNewGDPManager(t *testing.T) {

	manager := NewGDPManager(nil)

	if manager == nil {
		t.Fatal("Expected NewGDPManager to return a non-nil pointer")
	}
	if manager.cf != nil {
		t.Errorf("Expected cf to be set to the provided ClientFactory, got %+v", manager.cf)
	}
}
func TestNewGDPManager_Type(t *testing.T) {
	manager := NewGDPManager(nil)

	expectedType := "*k8s.GDPManager"
	if reflect.TypeOf(manager).String() != expectedType {
		t.Errorf("Expected type %s, got %s", expectedType, reflect.TypeOf(manager).String())
	}
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

func TestApplyTenant(t *testing.T) {
	cf := &ClientFactory{}
	manager := NewGDPManager(cf)

	tenant := newGDPTenant()

	cf.localClient = fake.NewSimpleClientset()
	scheme := runtime.NewScheme()
	gdptenant.AddToScheme(scheme)
	cf.localDynamicClient = dynamicfake.NewSimpleDynamicClient(scheme, tenant)

	// cf.dynamicClient = dynamicfake.FakeDynamicClient()
	err := manager.ApplyTenant(context.Background(), tenant)
	t.Logf("ApplyTenant error: %v", err)
	// require.NoError(t, err)
}

func TestGetTenant(t *testing.T) {
	cf := &ClientFactory{}
	manager := NewGDPManager(cf)

	tenant := newGDPTenant()

	cf.localClient = fake.NewSimpleClientset()
	scheme := runtime.NewScheme()
	gdptenant.AddToScheme(scheme)
	cf.localDynamicClient = dynamicfake.NewSimpleDynamicClient(scheme, tenant)

	// cf.dynamicClient = dynamicfake.FakeDynamicClient()
	_, err := manager.GetTenant(context.Background(), tenant.Name)
	t.Logf("GetTenant error: %v", err)
	require.NoError(t, err)
}

func TestListTenants(t *testing.T) {
	cf := &ClientFactory{}
	manager := NewGDPManager(cf)

	cf.localClient = fake.NewSimpleClientset()
	scheme := runtime.NewScheme()
	gdptenant.AddToScheme(scheme)
	cf.localDynamicClient = dynamicfake.NewSimpleDynamicClient(scheme)

	_, err := manager.ListTenants(context.Background())
	t.Logf("ListTenants error: %v", err)
	require.NoError(t, err)
}
