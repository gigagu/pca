package k8s

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"sync"

	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/homedir"

	gdptenant "TTOQPR/gdp-tenant/pkg/crd/api/v1"

	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
)

var (
	kubeconfig string
	configPath string
)

type ClientFactory struct {
	Configs            map[string]*rest.Config
	mu                 sync.RWMutex
	localClient        kubernetes.Interface
	localDynamicClient dynamic.Interface
	localConfig        *rest.Config
	GDPManager         *GDPManager
	dynamicClient      *dynamic.DynamicClient
	gdpMetrics         map[string]*gdptenant.GDPTenant
	minioEndpoint      string
	minioAccessKey     string
	minioSecretKey     string
}

func NewClientFactory() (*ClientFactory, error) {
	// connect k8s cluster
	if home := homedir.HomeDir(); home == "" {
		flag.StringVar(&kubeconfig, "kubeconfig", "", "absolute path to the kubeconfig file")
	} else {
		kubeconfig = filepath.Join(home, ".kube", "config")
	}

	config, err := rest.InClusterConfig()
	if err != nil {
		kubeconfig := clientcmd.NewDefaultClientConfigLoadingRules().GetDefaultFilename()
		config, err = clientcmd.BuildConfigFromFlags("", kubeconfig)
		if err != nil {
			return nil, fmt.Errorf("failed to create local cluster config: %w", err)
		}
	}
	localClient, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, fmt.Errorf("failed to create local cluster client: %w", err)
	}

	dynamicClient, err := dynamic.NewForConfig(config)

	if err != nil {
		return nil, fmt.Errorf("failed to create dynamic cluster client: %w", err)
	}

	cf := &ClientFactory{
		localClient:   localClient,
		dynamicClient: dynamicClient,
		localConfig:   config,
	}
	cf.GDPManager = NewGDPManager(cf)
	cf.localDynamicClient = dynamicClient
	cf.gdpMetrics = make(map[string]*gdptenant.GDPTenant)

	return cf, nil
}

func (cf *ClientFactory) SetLocalClient(client kubernetes.Interface) *ClientFactory {
	cf.localClient = client
	return cf
}

func (cf *ClientFactory) GetLocalClient() (*kubernetes.Clientset, error) {
	clientset, ok := cf.localClient.(*kubernetes.Clientset)
	if !ok {
		return nil, fmt.Errorf("localClient is not a *kubernetes.Clientset")
	}
	return clientset, nil
}

func (cf *ClientFactory) GetLocalDynamicClient() (dynamic.Interface, error) {
	if cf.localDynamicClient == nil {
		return nil, fmt.Errorf("localDynamicClient is nil")
	}
	return cf.localDynamicClient, nil
}

func (cf *ClientFactory) SetLocalDynamicClient(client dynamic.Interface) (dynamic.Interface, error) {
	cf.localDynamicClient = client
	return cf.localDynamicClient, nil
}

func (cf *ClientFactory) SetMinioCredential() *ClientFactory {
	// cf.minioEndpoint = "minio-os1-stg.50821.app.standardchartered.com:4161"
	cf.minioEndpoint = os.Getenv("MINIO_ENDPOINT")
	cf.minioAccessKey = os.Getenv("MINIO_ACCESS_KEY")
	cf.minioSecretKey = os.Getenv("MINO_SECRET_KEY")
	return cf
}

func (cf *ClientFactory) GetMinioCredential() (string, string, string) {
	return cf.minioEndpoint, cf.minioAccessKey, cf.minioSecretKey
}
