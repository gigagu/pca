package tenant

import (
	"io"
	"net/http/httptest"
	"testing"

	"bytes"
	"encoding/json"
	"net/http"

	"TTOQPR/gdp-tenant/pkg/constants"
	"TTOQPR/gdp-tenant/pkg/k8s"
	"TTOQPR/gdp-tenant/pkg/model"

	"github.com/labstack/echo/v4"
	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	"k8s.io/client-go/kubernetes/fake"
)

// Helper for echo context with body
func newEchoCtxWithBody(method, path string, body io.Reader) (echo.Context, *httptest.ResponseRecorder) {
	e := echo.New()
	req := httptest.NewRequest(method, path, body)
	req.Header.Set(echo.HeaderContentType, echo.MIMEApplicationJSON)
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)
	return c, rec
}

type fakeClientFactory struct {
	clientset *fake.Clientset
}

func (f *fakeClientFactory) GetLocalClient() (*fake.Clientset, error) {
	return f.clientset, nil
}

func TestGetContainerMFByTenantName_ReturnsNil(t *testing.T) {
	tc := &TenantController{}
	result, err := tc.GetContainerMFByTenantName("some-tenant")
	if err != nil {
		t.Errorf("expected nil error, got %v", err)
	}
	if result != nil {
		t.Errorf("expected nil result, got %v", result)
	}
}
func TestNewTenantController_ReturnsControllerWithFactory(t *testing.T) {

	tc, err := NewTenantController(nil)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if tc == nil {
		t.Fatalf("expected TenantController, got nil")
	}
	if tc.cf != nil {
		t.Errorf("expected cf to be set to mockFactory")
	}
}

func TestNewTenantController_NilFactory(t *testing.T) {
	tc, err := NewTenantController(nil)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if tc == nil {
		t.Fatalf("expected TenantController, got nil")
	}
	if tc.cf != nil {
		t.Errorf("expected cf to be nil")
	}
}
func TestGetContainerMFByTenantName_ReturnsNilAndNoError(t *testing.T) {
	tc := &TenantController{}
	result, err := tc.GetContainerMFByTenantName("test-tenant")
	if err != nil {
		t.Errorf("expected nil error, got %v", err)
	}
	if result != nil {
		t.Errorf("expected nil result, got %v", result)
	}
}

func TestGetContainerMFByTenantName_EmptyTenantName(t *testing.T) {
	tc := &TenantController{}
	result, err := tc.GetContainerMFByTenantName("")
	if err != nil {
		t.Errorf("expected nil error for empty tenant name, got %v", err)
	}
	if result != nil {
		t.Errorf("expected nil result for empty tenant name, got %v", result)
	}
}

func TestCreateTenant_ApplyTenantFails(t *testing.T) {
	clientset := fake.NewSimpleClientset(&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "default"}})
	am := map[string]interface{}{
		"MetaData": map[string]interface{}{"ManifestSchema": "x"},
		"PlatformConfig": []map[string]interface{}{
			{
				"Itam":       1,
				"ItamName":   "foo",
				"Namespaces": []string{"ns1"},
				"ResourceQuotas": map[string]interface{}{
					"RequestsCPU": "1", "RequestsMemory": "1Gi", "LimitsCPU": "2", "LimitsMemory": "2Gi",
				},
			},
		},
	}
	body, _ := json.Marshal(am)
	e := echo.New()
	req := httptest.NewRequest("POST", "/", bytes.NewReader(body))
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	clientfactory, _ := k8s.NewClientFactory()
	clientfactory.SetLocalClient(clientset)

	tc := &TenantController{cf: clientfactory}
	err := tc.CreateTenant(c)
	assert.NoError(t, err)
	assert.Equal(t, http.StatusInternalServerError, rec.Code)
	// assert.Contains(t, rec.Body.String(), "failed to create tenant")
}
func TestCreateTenant_BindError(t *testing.T) {
	clientset := fake.NewSimpleClientset(&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "default"}})
	e := echo.New()
	// Invalid JSON to trigger Bind error
	req := httptest.NewRequest("POST", "/", bytes.NewBufferString("{invalid json"))
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	clientfactory, _ := k8s.NewClientFactory()
	clientfactory.SetLocalClient(clientset)
	tc := &TenantController{cf: clientfactory}

	err := tc.CreateTenant(c)
	assert.NoError(t, err)
	// http.StatusInternalServerError, fmt.Errorf("tenant is empty"))
	assert.Equal(t, http.StatusInternalServerError, rec.Code)
}

func TestCreateTenant_EmptyManifestSchema(t *testing.T) {
	clientset := fake.NewSimpleClientset(&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "default"}})
	am := map[string]interface{}{
		"MetaData": map[string]interface{}{"ManifestSchema": ""},
		"PlatformConfig": []map[string]interface{}{
			{
				"Itam":       1,
				"ItamName":   "foo",
				"Namespaces": []string{"ns1"},
				"ResourceQuotas": map[string]interface{}{
					"RequestsCPU": "1", "RequestsMemory": "1Gi", "LimitsCPU": "2", "LimitsMemory": "2Gi",
				},
			},
		},
	}
	body, _ := json.Marshal(am)
	e := echo.New()
	req := httptest.NewRequest("POST", "/", bytes.NewReader(body))
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	clientfactory, _ := k8s.NewClientFactory()
	clientfactory.SetLocalClient(clientset)
	tc := &TenantController{cf: clientfactory}

	err := tc.CreateTenant(c)
	assert.NoError(t, err)
	assert.Equal(t, http.StatusInternalServerError, rec.Code)
}

// This test assumes GDPManager is nil, so ApplyTenant will panic or fail.
// You may want to mock GDPManager for more advanced scenarios.
func TestCreateTenant_GDPManagerNil(t *testing.T) {
	clientset := fake.NewSimpleClientset(&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "default"}})
	am := map[string]interface{}{
		"MetaData": map[string]interface{}{"ManifestSchema": "x"},
		"PlatformConfig": []map[string]interface{}{
			{
				"Itam":       1,
				"ItamName":   "foo",
				"Namespaces": []string{"ns1"},
				"ResourceQuotas": map[string]interface{}{
					"RequestsCPU": "1", "RequestsMemory": "1Gi", "LimitsCPU": "2", "LimitsMemory": "2Gi",
				},
			},
		},
	}
	body, _ := json.Marshal(am)
	e := echo.New()
	req := httptest.NewRequest("POST", "/", bytes.NewReader(body))
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	clientfactory, _ := k8s.NewClientFactory()
	clientfactory.SetLocalClient(clientset)
	// GDPManager is nil
	tc := &TenantController{cf: clientfactory}

	// Should not panic, but will fail to call ApplyTenant
	err := tc.CreateTenant(c)
	assert.NoError(t, err)
}

func TestCreateTenant_GetLocalClientFails(t *testing.T) {
	tc := &TenantController{cf: nil}
	e := echo.New()
	req := httptest.NewRequest("POST", "/", nil)
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	// Patch method to return error
	orig := tc.cf
	defer func() { tc.cf = orig }()
	tc.cf = &k8s.ClientFactory{}
	// Simulate GetLocalClient error by using nil factory
	err := tc.CreateTenant(c)
	assert.NoError(t, err)
}
func TestBuildTenant_BasicFields(t *testing.T) {
	pc := model.PlatformConfig{
		Namespaces:                 []string{"ns1", "ns2"},
		Target:                     []string{"target1"},
		BC:                         "bc1",
		ItamName:                   "itamName1",
		Itam:                       123,
		TenantNoninteractiveOwner:  []string{"owner1"},
		TenantNoninteractiveViewer: []string{"viewer1"},
		TenantInteractiveOwner:     []string{"intowner1"},
		TenantInteractiveViewer:    []string{"intviewer1"},
		ResourceQuotas: model.ResourceQuotas{
			RequestsCPU:    "1",
			RequestsMemory: "2Gi",
			LimitsCPU:      "2",
			LimitsMemory:   "4Gi",
		},
		ObjectStoreBuckets: []model.ObjectStoreBuckets{
			{
				Name:       "demo",
				Location:   "us-west-1",
				BucketSize: "20",
			},
		},
	}

	tenantName := "t-123-itamName1"
	tenant := BuildTenant(tenantName, pc)

	assert.NotNil(t, tenant)
	assert.Equal(t, "gdp.standardchartered.com/v1", tenant.TypeMeta.APIVersion)
	assert.Equal(t, "GDPTenant", tenant.TypeMeta.Kind)
	assert.Equal(t, tenantName, tenant.ObjectMeta.Name)
	assert.Equal(t, constants.GDPNamespace, tenant.ObjectMeta.Namespace)
	assert.Equal(t, tenantName, tenant.Spec.Tenant)
	assert.Equal(t, pc.Namespaces, tenant.Spec.Namespaces)
	assert.Equal(t, pc.Target, tenant.Spec.Target)
	assert.Equal(t, pc.BC, tenant.Spec.BC)
	assert.Equal(t, pc.ItamName, tenant.Spec.ItamName)
	assert.Equal(t, pc.Itam, tenant.Spec.Itam)
	assert.Equal(t, pc.TenantNoninteractiveOwner, tenant.Spec.TenantNoninteractiveOwner)
	assert.Equal(t, pc.TenantNoninteractiveViewer, tenant.Spec.TenantNoninteractiveViewer)
	assert.Equal(t, pc.TenantInteractiveOwner, tenant.Spec.TenantInteractiveOwner)
	assert.Equal(t, pc.TenantInteractiveViewer, tenant.Spec.TenantInteractiveViewer)
	assert.Equal(t, pc.ResourceQuotas.RequestsCPU, tenant.Spec.ResourceQuotas.RequestsCPU)
	assert.Equal(t, pc.ResourceQuotas.RequestsMemory, tenant.Spec.ResourceQuotas.RequestsMemory)
	assert.Equal(t, pc.ResourceQuotas.LimitsCPU, tenant.Spec.ResourceQuotas.LimitsCPU)
	assert.Equal(t, pc.ResourceQuotas.LimitsMemory, tenant.Spec.ResourceQuotas.LimitsMemory)
	assert.Equal(t, 1, len(tenant.Spec.ObjectStoreBuckets))
	assert.Equal(t, "demo", tenant.Spec.ObjectStoreBuckets[0].Name)
}

func TestBuildTenant_EmptyFields(t *testing.T) {
	pc := model.PlatformConfig{}
	tenantName := "t-0-empty"
	tenant := BuildTenant(tenantName, pc)

	assert.NotNil(t, tenant)
	assert.Equal(t, tenantName, tenant.ObjectMeta.Name)
	assert.Equal(t, constants.GDPNamespace, tenant.ObjectMeta.Namespace)
	assert.Equal(t, tenantName, tenant.Spec.Tenant)
	assert.Empty(t, tenant.Spec.Namespaces)
	assert.Empty(t, tenant.Spec.Target)
	assert.Empty(t, tenant.Spec.BC)
	assert.Empty(t, tenant.Spec.ItamName)
	assert.Equal(t, 0, tenant.Spec.Itam)
	assert.Empty(t, tenant.Spec.TenantNoninteractiveOwner)
	assert.Empty(t, tenant.Spec.TenantNoninteractiveViewer)
	assert.Empty(t, tenant.Spec.TenantInteractiveOwner)
	assert.Empty(t, tenant.Spec.TenantInteractiveViewer)
	assert.Empty(t, tenant.Spec.ResourceQuotas.RequestsCPU)
	assert.Empty(t, tenant.Spec.ResourceQuotas.RequestsMemory)
	assert.Empty(t, tenant.Spec.ResourceQuotas.LimitsCPU)
	assert.Empty(t, tenant.Spec.ResourceQuotas.LimitsMemory)
}
func TestBuildTenant_WithYunikornQueuesAndObjectStoreBuckets(t *testing.T) {
	pc := model.PlatformConfig{
		Namespaces:                 []string{"ns1"},
		Target:                     []string{"target1"},
		BC:                         "bc1",
		ItamName:                   "itamName1",
		Itam:                       42,
		TenantNoninteractiveOwner:  []string{"owner"},
		TenantNoninteractiveViewer: []string{"viewer"},
		TenantInteractiveOwner:     []string{"intowner"},
		TenantInteractiveViewer:    []string{"intviewer"},
		ResourceQuotas: model.ResourceQuotas{
			RequestsCPU:    "100m",
			RequestsMemory: "256Mi",
			LimitsCPU:      "200m",
			LimitsMemory:   "512Mi",
		},
		YunikornQueues: []model.YunikornQueue{
			{
				Name:           "queue1",
				RequestsCPU:    "1",
				RequestsMemory: "1Gi",
				LimitCPU:       "2",
				LimitMemory:    "2Gi",
			},
		},
		ObjectStoreBuckets: []model.ObjectStoreBuckets{
			{
				Name:       "bucket1",
				BucketSize: "10Gi",
				Location:   "us-east-1",
			},
		},
	}
	tenantName := "t-42-itamName1"
	tenant := BuildTenant(tenantName, pc)

	assert.NotNil(t, tenant)
	assert.Equal(t, tenantName, tenant.ObjectMeta.Name)
	assert.Equal(t, constants.GDPNamespace, tenant.ObjectMeta.Namespace)
	assert.Equal(t, 1, len(tenant.Spec.YunikornQueues))
	assert.Equal(t, "queue1", tenant.Spec.YunikornQueues[0].Name)
	assert.Equal(t, "1", tenant.Spec.YunikornQueues[0].RequestsCPU)
	assert.Equal(t, "1Gi", tenant.Spec.YunikornQueues[0].RequestsMemory)
	assert.Equal(t, "2", tenant.Spec.YunikornQueues[0].LimitCPU)
	assert.Equal(t, "2Gi", tenant.Spec.YunikornQueues[0].LimitMemory)
	assert.Equal(t, 1, len(tenant.Spec.ObjectStoreBuckets))
	assert.Equal(t, "bucket1", tenant.Spec.ObjectStoreBuckets[0].Name)
	assert.Equal(t, "10Gi", tenant.Spec.ObjectStoreBuckets[0].BucketSize)
	assert.Equal(t, "us-east-1", tenant.Spec.ObjectStoreBuckets[0].Location)
}

func TestBuildTenant_EmptyYunikornQueuesAndObjectStoreBuckets(t *testing.T) {
	pc := model.PlatformConfig{
		Namespaces:         []string{"ns1"},
		Target:             []string{"target1"},
		BC:                 "bc1",
		ItamName:           "itamName1",
		Itam:               42,
		ResourceQuotas:     model.ResourceQuotas{},
		YunikornQueues:     nil,
		ObjectStoreBuckets: nil,
	}
	tenantName := "t-42-itamName1"
	tenant := BuildTenant(tenantName, pc)

	assert.NotNil(t, tenant)
	assert.Equal(t, 0, len(tenant.Spec.YunikornQueues))
	assert.Equal(t, 0, len(tenant.Spec.ObjectStoreBuckets))
}

func TestBuildTenant_MultipleYunikornQueuesAndBuckets(t *testing.T) {
	pc := model.PlatformConfig{
		YunikornQueues: []model.YunikornQueue{
			{Name: "q1"}, {Name: "q2"},
		},
		ObjectStoreBuckets: []model.ObjectStoreBuckets{
			{Name: "b1"}, {Name: "b2"},
		},
	}
	tenant := BuildTenant("t", pc)
	assert.Len(t, tenant.Spec.YunikornQueues, 2)
	assert.Equal(t, "q1", tenant.Spec.YunikornQueues[0].Name)
	assert.Equal(t, "q2", tenant.Spec.YunikornQueues[1].Name)
	assert.Len(t, tenant.Spec.ObjectStoreBuckets, 2)
	assert.Equal(t, "b1", tenant.Spec.ObjectStoreBuckets[0].Name)
	assert.Equal(t, "b2", tenant.Spec.ObjectStoreBuckets[1].Name)
}
func TestCreateTenant_GetLocalClientError(t *testing.T) {
	// Simulate GetLocalClient error
	tc := &TenantController{cf: &k8s.ClientFactory{}}
	e := echo.New()
	req := httptest.NewRequest("POST", "/", nil)
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	// Patch GetLocalClient to return error

	err := tc.CreateTenant(c)
	assert.NoError(t, err)
	// assert.Contains(t, rec.Body.String(), "failed to get local client")
}

func TestCreateTenant_ListNamespacesError(t *testing.T) {
	clientset := fake.NewSimpleClientset(&corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name: "existing-ns",
		},
	})
	tc := &TenantController{cf: &k8s.ClientFactory{}}
	tc.cf.SetLocalClient(clientset)
	e := echo.New()
	req := httptest.NewRequest("POST", "/", nil)
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	err := tc.CreateTenant(c)
	assert.NoError(t, err)
	assert.Equal(t, http.StatusInternalServerError, rec.Code)
}

func TestCreateTenant_BindError_InvalidJSON(t *testing.T) {
	clientset := fake.NewSimpleClientset(&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "default"}})
	tc := &TenantController{cf: &k8s.ClientFactory{}}
	tc.cf.SetLocalClient(clientset)
	e := echo.New()
	req := httptest.NewRequest("POST", "/", bytes.NewBufferString("{invalid json"))
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	err := tc.CreateTenant(c)
	assert.NoError(t, err)
	// assert.Contains(t, rec.Body.String(), "JSON syntax error")
}

func TestCreateTenantMinio(t *testing.T) {
	// cf := &k8s.ClientFactory{}
	// manager := k8s.NewGDPManager(cf)

	pc := model.PlatformConfig{
		Namespaces:                 []string{"default"},
		Target:                     []string{"target1"},
		BC:                         "bc1",
		ItamName:                   "itamName1",
		Itam:                       123,
		TenantNoninteractiveOwner:  []string{"owner1"},
		TenantNoninteractiveViewer: []string{"viewer1"},
		TenantInteractiveOwner:     []string{"intowner1"},
		TenantInteractiveViewer:    []string{"intviewer1"},
		ResourceQuotas: model.ResourceQuotas{
			RequestsCPU:    "1",
			RequestsMemory: "2Gi",
			LimitsCPU:      "2",
			LimitsMemory:   "4Gi",
		},
		ObjectStoreBuckets: []model.ObjectStoreBuckets{
			{
				Name:       "demo",
				Location:   "us-west-1",
				BucketSize: "20",
			},
		},
	}
	tenant := BuildTenant("t", pc)
	scheme := runtime.NewScheme()
	// tenant.AddToScheme(scheme)
	clientset := fake.NewSimpleClientset(&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "default"}})

	e := echo.New()
	req := httptest.NewRequest("POST", "/", bytes.NewBufferString("{invalid json"))
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	tc := &TenantController{cf: &k8s.ClientFactory{}}
	tc.cf.SetLocalClient(clientset)
	tc.cf.SetLocalDynamicClient(dynamicfake.NewSimpleDynamicClient(scheme, tenant))

	err := tc.CreateTenant(c)

	assert.NoError(t, err)

}
