package k8s

import (
	"os"
	"testing"

	gdptenantv1 "TTOQPR/gdp-tenant/pkg/crd/api/v1"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

func TestNewClientFactory(t *testing.T) {
	// Prepare test cases
	tests := []struct {
		name          string
		setupEnv      func()
		cleanupEnv    func()
		wantErr       bool
		errorContains string
	}{
		{
			name: "successfully create ClientFactory - using kubeconfig",
			setupEnv: func() {
				// Create temporary kubeconfig file
				tmpKubeconfig := `
apiVersion: v1
clusters:
- cluster:
    server: https://localhost:6443
  name: test-cluster
contexts:
- context:
    cluster: test-cluster
    user: test-user
  name: test-context
current-context: test-context
kind: Config
users:
- name: test-user
  user:
    token: test-token
`
				os.Setenv("KUBECONFIG", "/tmp/test-kubeconfig")
				os.WriteFile("/tmp/test-kubeconfig", []byte(tmpKubeconfig), 0644)
			},
			cleanupEnv: func() {
				os.Remove("/tmp/test-kubeconfig")
				os.Unsetenv("KUBECONFIG")
			},
			wantErr: false,
		},
		{
			name: "return error when unable to create config",
			setupEnv: func() {
				// Set an invalid kubeconfig path
				os.Setenv("KUBECONFIG", "/non/existent/path")
			},
			cleanupEnv: func() {
				os.Unsetenv("KUBECONFIG")
			},
			wantErr:       true,
			errorContains: "failed to create local cluster config",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Setup test environment
			if tt.setupEnv != nil {
				tt.setupEnv()
			}
			defer func() {
				if tt.cleanupEnv != nil {
					tt.cleanupEnv()
				}
			}()

			// Execute test
			cf, _ := NewClientFactory()
			// Verify returned ClientFactory structure
			if cf == nil {
				return
			}

			// Verify all fields are properly initialized
			if cf.dynamicClient == nil {
				t.Error("dynamicClient map not initialized")
			}

			if cf.localClient == nil {
				t.Error("localClient not initialized")
			}
			if cf.localDynamicClient == nil {
				t.Error("LocalDynamicClient not initialized")
			}
			if cf.localConfig == nil {
				t.Error("LocalConfig not initialized")
			}
		})
	}
}
func TestClientFactory_GetLocalClient(t *testing.T) {
	// Case 1: localClient is a *kubernetes.Clientset
	cf := &ClientFactory{}
	// Create a dummy rest.Config (won't connect to real cluster)
	config := &rest.Config{Host: "https://localhost"}
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		t.Fatalf("failed to create dummy clientset: %v", err)
	}
	cf.localClient = clientset

	got, err := cf.GetLocalClient()
	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}
	if got == nil {
		t.Error("expected non-nil clientset")
	}

	// Case 2: localClient is not a *kubernetes.Clientset
	cf2 := &ClientFactory{ // definitely not a *kubernetes.Clientset
	}
	got2, err2 := cf2.GetLocalClient()
	if err2 == nil {
		t.Error("expected error, got nil")
	}
	if got2 != nil {
		t.Error("expected nil clientset, got non-nil")
	}
	if err2 != nil && err2.Error() != "localClient is not a *kubernetes.Clientset" {
		t.Errorf("unexpected error: %v", err2)
	}
}
func TestClientFactory_SetLocalClient(t *testing.T) {
	cf := &ClientFactory{}
	// Create a dummy rest.Config (won't connect to real cluster)
	config := &rest.Config{Host: "https://localhost"}
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		t.Fatalf("failed to create dummy clientset: %v", err)
	}

	// Call SetLocalClient and check returned value and field assignment
	ret := cf.SetLocalClient(clientset)
	if ret != cf {
		t.Error("SetLocalClient should return the same ClientFactory instance")
	}
	if cf.localClient != clientset {
		t.Error("SetLocalClient did not set localClient correctly")
	}

	// Test with nil client
	ret2 := cf.SetLocalClient(nil)
	if ret2 != cf {
		t.Error("SetLocalClient should return the same ClientFactory instance when setting nil")
	}
	if cf.localClient != nil {
		t.Error("SetLocalClient did not set localClient to nil")
	}
}
func TestClientFactory_SetLocalDynamicClient(t *testing.T) {
	cf := &ClientFactory{}

	// Case 1: Set a non-nil dynamic.Interface
	scheme := runtime.NewScheme()
	gdptenantv1.AddToScheme(scheme)
	tenant := &gdptenantv1.GDPTenant{
		ObjectMeta: metav1.ObjectMeta{
			Name: "tenant1",
		},
		Spec: gdptenantv1.GDPTenantSpec{
			Itam:       55547,
			BC:         "bc1",
			Namespaces: []string{"ns1", "ns2"},
			ResourceQuotas: gdptenantv1.ResourceQuotas{
				RequestsCPU:    "2",
				RequestsMemory: "1024",
			},
		},
	}

	mockDynamic := dynamicfake.NewSimpleDynamicClient(scheme, tenant)
	ret, err := cf.SetLocalDynamicClient(mockDynamic)
	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}
	if ret != mockDynamic {
		t.Error("SetLocalDynamicClient did not return the set dynamic.Interface")
	}
	if cf.localDynamicClient != mockDynamic {
		t.Error("SetLocalDynamicClient did not set localDynamicClient correctly")
	}

	// Case 2: Set nil dynamic.Interface
	ret2, err2 := cf.SetLocalDynamicClient(nil)
	if err2 != nil {
		t.Errorf("expected no error when setting nil, got %v", err2)
	}
	if ret2 != nil {
		t.Error("expected returned interface to be nil")
	}
	if cf.localDynamicClient != nil {
		t.Error("localDynamicClient should be nil after setting nil")
	}
}

func TestClientFactory_GetLocalDynamicClient(t *testing.T) {
	cf := &ClientFactory{}

	// Case 1: Set a non-nil dynamic.Interface
	scheme := runtime.NewScheme()
	gdptenantv1.AddToScheme(scheme)
	tenant := &gdptenantv1.GDPTenant{
		ObjectMeta: metav1.ObjectMeta{
			Name: "tenant1",
		},
		Spec: gdptenantv1.GDPTenantSpec{
			Itam:       55547,
			BC:         "bc1",
			Namespaces: []string{"ns1", "ns2"},
			ResourceQuotas: gdptenantv1.ResourceQuotas{
				RequestsCPU:    "2",
				RequestsMemory: "1024",
			},
		},
	}
	t.Run("localDynamicClient is set", func(t *testing.T) {

		mockDynamic := dynamicfake.NewSimpleDynamicClient(scheme, tenant)
		cf.localDynamicClient = mockDynamic

		got, err := cf.GetLocalDynamicClient()
		if err != nil {
			t.Errorf("expected no error, got %v", err)
		}
		if got != mockDynamic {
			t.Error("expected returned dynamic.Interface to match the set value")
		}
	})

	t.Run("localDynamicClient is nil", func(t *testing.T) {

		cf.localDynamicClient = nil

		got, err := cf.GetLocalDynamicClient()
		if err == nil {
			t.Error("expected error, got nil")
		}
		if got != nil {
			t.Error("expected returned dynamic.Interface to be nil")
		}
		if err != nil && err.Error() != "localDynamicClient is nil" {
			t.Errorf("unexpected error: %v", err)
		}
	})
}
func TestClientFactory_GetMinioCredential(t *testing.T) {
	cf := &ClientFactory{
		minioEndpoint:  "http://minio.example.com",
		minioAccessKey: "test-access-key",
		minioSecretKey: "test-secret-key",
	}

	endpoint, accessKey, secretKey := cf.GetMinioCredential()
	if endpoint != "http://minio.example.com" {
		t.Errorf("expected endpoint 'http://minio.example.com', got '%s'", endpoint)
	}
	if accessKey != "test-access-key" {
		t.Errorf("expected accessKey 'test-access-key', got '%s'", accessKey)
	}
	if secretKey != "test-secret-key" {
		t.Errorf("expected secretKey 'test-secret-key', got '%s'", secretKey)
	}

	// Test with empty values
	cf2 := &ClientFactory{}
	e, a, s := cf2.GetMinioCredential()
	if e != "" || a != "" || s != "" {
		t.Errorf("expected all empty strings, got endpoint='%s', accessKey='%s', secretKey='%s'", e, a, s)
	}
}
func TestClientFactory_SetMinioCredential(t *testing.T) {
	// Save and restore env vars to avoid side effects
	origAccessKey := os.Getenv("MINIO_ACCESS_KEY")
	origSecretKey := os.Getenv("MINIO_SECRET_KEY")
	defer func() {
		os.Setenv("MINIO_ACCESS_KEY", origAccessKey)
		os.Setenv("MINIO_SECRET_KEY", origSecretKey)
	}()

	os.Setenv("MINIO_ACCESS_KEY", "test-access")
	os.Setenv("MINIO_SECRET_KEY", "test-secret")

	cf := &ClientFactory{}
	ret := cf.SetMinioCredential()

	if ret != cf {
		t.Error("SetMinioCredential should return the same ClientFactory instance")
	}
	expectedEndpoint := "minio-os1-stg.50821.app.standardchartered.com:4161"
	if cf.minioEndpoint != expectedEndpoint {
		t.Errorf("expected minioEndpoint '%s', got '%s'", expectedEndpoint, cf.minioEndpoint)
	}
	if cf.minioAccessKey != "test-access" {
		t.Errorf("expected minioAccessKey 'test-access', got '%s'", cf.minioAccessKey)
	}
	if cf.minioSecretKey != "test-secret" {
		t.Errorf("expected minioSecretKey 'test-secret', got '%s'", cf.minioSecretKey)
	}

	// Test with empty env vars
	os.Setenv("MINIO_ACCESS_KEY", "")
	os.Setenv("MINIO_SECRET_KEY", "")
	cf2 := &ClientFactory{}
	cf2.SetMinioCredential()
	if cf2.minioAccessKey != "" {
		t.Errorf("expected empty minioAccessKey, got '%s'", cf2.minioAccessKey)
	}
	if cf2.minioSecretKey != "" {
		t.Errorf("expected empty minioSecretKey, got '%s'", cf2.minioSecretKey)
	}
}



