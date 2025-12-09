package tenant

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"

	"TTOQPR/gdp-tenant/pkg/constants"

	"crypto/tls"
	"encoding/base64"

	"k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"
)

type Trino struct{}

var insecureClient = &http.Client{
	Transport: &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	},
}

func (t *Trino) GetResourceGroup(tenantName string, encoded string) (string, error) {
	// step1: read all resource groups, find id by name
	url := constants.TrinoEndpoint + "/trino/resourcegroup/read"
	req, _ := http.NewRequest("GET", url, nil)
	req.Header.Set("Authorization", "Basic "+encoded)

	// resp, err := http.DefaultClient.Do(req)
	resp, err := insecureClient.Do(req)
	if err != nil {
		klog.Errorf("failed to read resource groups: %v", err)
		return "", fmt.Errorf("failed to read resource groups: %w", err)
	}
	defer resp.Body.Close()

	var groups []map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&groups); err != nil {
		klog.Errorf("decode resource groups failed: %v", err)
		return "", fmt.Errorf("decode resource groups failed: %w", err)
	}

	for _, g := range groups {
		if g["name"] == tenantName {
			switch id := g["resourceGroupId"].(type) {
			case float64:
				return fmt.Sprintf("%.0f", id), nil
			case int:
				return fmt.Sprintf("%d", id), nil
			case string:
				return id, nil
			}
		}
	}
	klog.Infof("resource group not found for name: %s", tenantName)
	return "", nil
}

func (t *Trino) CreateResourceGroup(tenantName string, encoded string, TenantNoninteractiveOwner []string) error {
	url := constants.TrinoEndpoint + "/trino/resourcegroup/create"
	resourceGroupReq := map[string]interface{}{
		"name":                 tenantName,
		"softMemoryLimit":      "30%",
		"maxQueued":            30,
		"softConcurrencyLimit": 20,
		"hardConcurrencyLimit": 20,
		"jmxExport":            true,
		"parent":               1,
		"environment":          "trinoskestg",
	}
	body, _ := json.Marshal(resourceGroupReq)
	req, _ := http.NewRequest("POST", url, bytes.NewBuffer(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Basic "+encoded)

	// resp, err := http.DefaultClient.Do(req)
	resp, err := insecureClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to create resource group: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := ioutil.ReadAll(resp.Body)
		klog.Errorf("response body: %s", string(b))
		return fmt.Errorf("resource group create failed: %s", string(b))
	}

	groupID, err := t.GetResourceGroup(tenantName, encoded)
	if err != nil {
		return fmt.Errorf("failed to get resource group: %w", err)
	}
	// step: create selector
	selectorReq := map[string]interface{}{}
	for _, owner := range TenantNoninteractiveOwner {
		selectorReq = map[string]interface{}{
			"resourceGroupId": groupID,
			"priority":        2,
			"userRegex":       owner,
		}

	}
	bodySel, _ := json.Marshal(selectorReq)
	// selectorReq := map[string]interface{}{
	// 	"resourceGroupId": groupID,
	// 	"priority":        2,
	// 	"userRegex":       tenantName,
	// }
	// bodySel, _ := json.Marshal(selectorReq)

	req3, _ := http.NewRequest("POST", constants.TrinoEndpoint+"/trino/selector/create", bytes.NewBuffer(bodySel))
	req3.Header.Set("Content-Type", "application/json")
	req3.Header.Set("Authorization", "Basic "+encoded)
	// resp3, err := http.DefaultClient.Do(req3)
	resp3, err := insecureClient.Do(req3)
	if err != nil {
		klog.Errorf("failed to create selector: %v", err)
		return fmt.Errorf("failed to create selector: %w", err)
	}
	defer resp3.Body.Close()
	if resp3.StatusCode != http.StatusOK {
		b, _ := ioutil.ReadAll(resp3.Body)
		klog.Errorf("response body: %s", string(b))
		return fmt.Errorf("selector create failed: %s", string(b))
	}
	return nil
}

func (t *Trino) Apply(ctx context.Context, clientset *kubernetes.Clientset, tenantName string, TenantNoninteractiveOwner, TenantInteractiveOwner []string) error {
	// username := "g.gdprwpf.001.dev"
	// password := os.Getenv("TRINO_PASS")
	username := os.Getenv("TRINO_USER")
	password := os.Getenv("TRINO_PASSWORD")
	auth := username + ":" + password
	encoded := base64.StdEncoding.EncodeToString([]byte(auth))

	groupID, err := t.GetResourceGroup(tenantName, encoded)
	if err != nil {
		klog.Errorf("failed to get resource group: %v", err)
		return fmt.Errorf("failed to get resource group: %w", err)
	}
	klog.Info("resource group id: ", groupID)

	if groupID == "" {
		// create resourceGroup
		klog.Info("creating resource group for tenant: ", tenantName)
		t.CreateResourceGroup(tenantName, encoded, TenantNoninteractiveOwner)
	} else {
		// update selector
		klog.Info("resource group already exists for tenant: ", tenantName)
		t.UpdateSelector(groupID, tenantName, encoded, TenantNoninteractiveOwner)
	}

	return nil
}

func (t *Trino) UpdateSelector(groupID, tenantName string, encoded string, TenantNoninteractiveOwner []string) error {
	selectorReq := map[string]interface{}{}
	for _, owner := range TenantNoninteractiveOwner {
		selectorReq = map[string]interface{}{
			"resourceGroupId": groupID,
			"priority":        2,
			"userRegex":       owner,
		}

	}
	bodySel, _ := json.Marshal(selectorReq)
	req, _ := http.NewRequest("POST", constants.TrinoEndpoint+"/trino/selector/update", bytes.NewBuffer(bodySel))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Basic "+encoded)
	// resp, err := http.DefaultClient.Do(req)
	resp, err := insecureClient.Do(req)
	if err != nil {
		klog.Errorf("failed to create selector: %v", err)
		return fmt.Errorf("failed to create selector: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := ioutil.ReadAll(resp.Body)
		// klog.Errorf("response body: %v", b)
		return fmt.Errorf("selector create failed: %s", string(b))
	}
	return nil
}
