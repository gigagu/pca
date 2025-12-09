package tenant

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"os"

	"k8s.io/klog"
)

const (
	// baseURL = "https://ranger-stg.55547.app.standardchartered.com"
	baseURL = "https://ranger-prd.sked011.55547.app.standardchartered.com"
)

type PolicyAccess struct {
	Type      string `json:"type"`
	IsAllowed bool   `json:"isAllowed"`
}

type PolicyItem struct {
	Accesses      []PolicyAccess `json:"accesses"`
	Groups        []string       `json:"groups,omitempty"`
	DelegateAdmin bool           `json:"delegateAdmin"`
	Users         []string       `json:"users,omitempty"`
}

type PolicyResource struct {
	Values      []string `json:"values"`
	IsExcludes  bool     `json:"isExcludes"`
	IsRecursive bool     `json:"isRecursive"`
}

type PolicyRequest struct {
	IsEnabled      bool                      `json:"isEnabled"`
	Service        string                    `json:"service"`
	Name           string                    `json:"name"`
	PolicyType     int                       `json:"policyType"`
	PolicyPriority int                       `json:"policyPriority"`
	Description    string                    `json:"description,omitempty"`
	IsAuditEnabled bool                      `json:"isAuditEnabled"`
	Resources      map[string]PolicyResource `json:"resources"`
	PolicyItems    []PolicyItem              `json:"policyItems"`
	ServiceType    string                    `json:"serviceType"`
	IsDenyAllElse  bool                      `json:"isDenyAllElse"`
}

type Ranger struct{}

func (r *Ranger) Apply(tenantName, schemaName, schemaPath, dbName string, adminGroups, viewerGroups []string) error {
	if len(schemaPath) >= 6 && schemaPath[:6] == "s3a://" {
		schemaPath = schemaPath[4:]
	}

	// errCh := make(chan error, 4)

	// go func() {
	// 	errCh <- CreateS3AdminPolicy(tenantName, schemaName, schemaPath, adminGroups)
	// }()
	// go func() {
	// 	errCh <- CreateTableAdminPolicy(tenantName, schemaName, dbName, adminGroups)
	// }()
	// go func() {
	// 	errCh <- CreateS3ViewerPolicy(tenantName, dbName, schemaPath, viewerGroups)
	// }()
	// go func() {
	// 	errCh <- CreateTableViewerPolicy(tenantName, dbName, viewerGroups)
	// }()

	// var firstErr error
	// for i := 0; i < 4; i++ {
	// 	if err := <-errCh; err != nil && firstErr == nil {
	// 		firstErr = err
	// 		klog.Errorf("failed to apply ranger policy: %v", err)
	// 	}
	// }
	// if firstErr != nil {
	// 	return firstErr
	// }
	err := CreateS3AdminPolicy(tenantName, schemaName, schemaPath, adminGroups)
	if err != nil {
		// return err
		klog.Errorf("failed to apply ranger CreateS3AdminPolicy policy: %v", err)
	}
	err = CreateTableAdminPolicy(tenantName, schemaName, dbName, adminGroups)
	if err != nil {
		// return err
		klog.Errorf("failed to apply ranger CreateTableAdminPolicy policy: %v", err)
	}
	err = CreateS3ViewerPolicy(tenantName, dbName, schemaPath, viewerGroups)
	if err != nil {
		// return err
		klog.Errorf("failed to apply ranger CreateS3ViewerPolicy policy: %v", err)
	}
	err = CreateTableViewerPolicy(tenantName, dbName, viewerGroups)
	if err != nil {
		// return err
		klog.Errorf("failed to apply ranger CreateTableViewerPolicy policy: %v", err)
	}
	// err = CreateTrinoTableAdminPolicy(tenantName, schemaName, dbName, adminGroups)
	// if err != nil {
	// 	return err
	// }
	err = CreateTrinoTableAdminPolicy(tenantName, schemaName, adminGroups)
	if err != nil {
		// return err
		klog.Errorf("failed to apply ranger CreateTrinoTableAdminPolicy policy: %v", err)
	}
	err = CreateTrinoTableViewerPolicy(tenantName, schemaName, viewerGroups)
	if err != nil {
		// return err
		klog.Errorf("failed to apply ranger CreateTrinoTableViewerPolicy policy: %v", err)
	}
	// err = CreateTrinoTableViewerPolicy(tenantName, schemaName, dbName, viewerGroups)
	// if err != nil {
	// 	return err
	// }

	klog.Infof("Ranger policies applied for tenant: %s", tenantName)
	return nil
}

func postPolicy(policy PolicyRequest, funcName string) error {
	url := baseURL + "/service/plugins/policies/apply"
	klog.Info("update ranger policy ", policy)
	body, err := json.Marshal(policy)
	if err != nil {
		return fmt.Errorf("%v failed to marshal policy: %w", funcName, err)
	}
	jsonBytes, err := json.MarshalIndent(policy, "", "  ")
	if err != nil {
		klog.Error(funcName, "marshal error: ", err)
	} else {
		klog.Info(funcName, "policy json:\n", string(jsonBytes))
	}

	req, err := http.NewRequest("POST", url, bytes.NewBuffer(body))
	if err != nil {
		return fmt.Errorf("%v failed to create HTTP request: %w", funcName, err)
	}
	req.Header.Set("Content-Type", "application/json")
	user := os.Getenv("RANGER_USER")
	pass := os.Getenv("RANGER_PASS")
	req.SetBasicAuth(user, pass)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("%v Ranger policy DefaultClient failed: %s", funcName, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		return fmt.Errorf("%v Ranger policy apply failed: %s", funcName, resp.Status)
	}
	return nil
}

// Step 1: S3 path read/write for admin group
func CreateS3AdminPolicy(tenantName, schemaName, schemaPath string, adminGroups []string) error {
	policy := PolicyRequest{
		IsEnabled:      true,
		Service:        "fors3",
		Name:           fmt.Sprintf("admin-%s-%s", tenantName, schemaName),
		PolicyType:     0,
		PolicyPriority: 0,
		IsAuditEnabled: true,
		Resources: map[string]PolicyResource{
			"path": {
				Values:      []string{schemaPath},
				IsExcludes:  false,
				IsRecursive: true,
			},
		},
		PolicyItems: []PolicyItem{
			{
				Accesses: []PolicyAccess{
					{"read", true},
					{"write", true},
					{"execute", true},
				},
				Groups:        adminGroups,
				DelegateAdmin: false,
			},
		},
		ServiceType:   "hdfs",
		IsDenyAllElse: false,
	}
	return postPolicy(policy, "CreateS3AdminPolicy")
}

// Step 2: Database read/write for admin group
func CreateTableAdminPolicy(tenantName, schemaName, dbName string, adminGroups []string) error {
	policy := PolicyRequest{
		IsEnabled:      true,
		Service:        "hive-dev-policy",
		Name:           fmt.Sprintf("admin-%s-%s-tables", tenantName, schemaName),
		PolicyType:     0,
		PolicyPriority: 0,
		Description:    "Policy for admin - database",
		IsAuditEnabled: true,
		Resources: map[string]PolicyResource{
			"database": {
				Values:      []string{dbName},
				IsExcludes:  false,
				IsRecursive: false,
			},
			"table": {
				Values:      []string{"*"},
				IsExcludes:  false,
				IsRecursive: false,
			},
		},
		PolicyItems: []PolicyItem{
			{
				Accesses: []PolicyAccess{
					{"select", true}, {"update", true}, {"create", true}, {"drop", true},
					{"alter", true}, {"index", true}, {"lock", true}, {"all", true},
					{"read", true}, {"write", true}, {"repladmin", true}, {"serviceadmin", true},
					{"tempudfadmin", true}, {"refresh", true},
				},
				Groups:        adminGroups,
				DelegateAdmin: true,
			},
		},
		ServiceType:   "hive",
		IsDenyAllElse: false,
	}
	return postPolicy(policy, "CreateTableAdminPolicy")
}

// Step 3: S3 path read only for viewer group
func CreateS3ViewerPolicy(tenantName, dbName, schemaPath string, viewerGroups []string) error {
	policy := PolicyRequest{
		IsEnabled:      true,
		Service:        "fors3",
		Name:           fmt.Sprintf("view-%s-%s-path", tenantName, dbName),
		PolicyType:     0,
		PolicyPriority: 0,
		IsAuditEnabled: true,
		Resources: map[string]PolicyResource{
			"path": {
				Values:      []string{schemaPath},
				IsExcludes:  false,
				IsRecursive: true,
			},
		},
		PolicyItems: []PolicyItem{
			{
				Accesses: []PolicyAccess{
					{"read", true},
				},
				Groups:        viewerGroups,
				DelegateAdmin: false,
			},
		},
		ServiceType:   "hdfs",
		IsDenyAllElse: false,
	}
	return postPolicy(policy, "CreateS3ViewerPolicy")
}

// Step 4: Database table read only for viewer group
func CreateTableViewerPolicy(tenantName, dbName string, viewerGroups []string) error {
	policy := PolicyRequest{
		IsEnabled:      true,
		Service:        "hive-dev-policy",
		Name:           fmt.Sprintf("view-%s-%s-tables", tenantName, dbName),
		PolicyType:     0,
		PolicyPriority: 0,
		Description:    "Policy for all - database",
		IsAuditEnabled: true,
		Resources: map[string]PolicyResource{
			"database": {
				Values:      []string{dbName},
				IsExcludes:  false,
				IsRecursive: false,
			},
			"table": {
				Values:      []string{"*"},
				IsExcludes:  false,
				IsRecursive: false,
			},
		},
		PolicyItems: []PolicyItem{
			{
				Accesses: []PolicyAccess{
					{"select", true},
				},
				Groups:        viewerGroups,
				DelegateAdmin: false,
			},
		},
		ServiceType:   "hive",
		IsDenyAllElse: false,
	}
	return postPolicy(policy, "CreateTableViewerPolicy")
}

// Step 5: Trino table admin policy for admin group
func CreateTrinoTableAdminPolicy(tenantName, schemaName string, adminGroups []string) error {
	policy := PolicyRequest{
		IsEnabled: true,
		// Service:        "trinodev",
		Service:        "trino_prod",
		Name:           fmt.Sprintf("admin-%s-%s-tables", tenantName, schemaName),
		PolicyType:     0,
		PolicyPriority: 0,
		Description:    "Policy for admin - database",
		IsAuditEnabled: true,
		Resources: map[string]PolicyResource{
			"schema": {
				Values:      []string{schemaName},
				IsExcludes:  false,
				IsRecursive: false,
			},
			"catalog": {
				Values:      []string{"gdp_global"},
				IsExcludes:  false,
				IsRecursive: false,
			},
			"column": {
				Values:      []string{"*"},
				IsExcludes:  false,
				IsRecursive: false,
			},
			"table": {
				Values:      []string{"*"},
				IsExcludes:  false,
				IsRecursive: false,
			},
		},
		PolicyItems: []PolicyItem{
			{
				Accesses: []PolicyAccess{
					{"select", true}, {"insert", true}, {"create", true}, {"drop", true},
					{"delete", true}, {"use", true}, {"alter", true}, {"grant", true},
					{"revoke", true}, {"show", true}, {"impersonate", true}, {"all", true},
					{"execute", true}, {"read_sysinfo", true}, {"write_sysinfo", true},
				},
				Groups:        adminGroups,
				DelegateAdmin: true,
			},
		},
		ServiceType:   "trino",
		IsDenyAllElse: false,
	}
	return postPolicy(policy, "CreateTrinoTableAdminPolicy")
}

// Step 6: Trino table viewer policy for viewer group
func CreateTrinoTableViewerPolicy(tenantName, schemaName string, viewerGroups []string) error {
	policy := PolicyRequest{
		IsEnabled:      true,
		Service:        "trino_prod",
		Name:           fmt.Sprintf("viewer-%s-%s-tables", tenantName, schemaName),
		PolicyType:     0,
		PolicyPriority: 0,
		Description:    "Policy for admin - database",
		IsAuditEnabled: true,
		Resources: map[string]PolicyResource{
			"schema": {
				Values:      []string{schemaName},
				IsExcludes:  false,
				IsRecursive: false,
			},
			"catalog": {
				Values:      []string{"gdp_global"},
				IsExcludes:  false,
				IsRecursive: false,
			},
			"column": {
				Values:      []string{"*"},
				IsExcludes:  false,
				IsRecursive: false,
			},
			"table": {
				Values:      []string{"*"},
				IsExcludes:  false,
				IsRecursive: false,
			},
		},
		PolicyItems: []PolicyItem{
			{
				Accesses: []PolicyAccess{
					{"select", true},
					{"show", true},
				},
				Groups:        viewerGroups,
				DelegateAdmin: true,
			},
		},
		ServiceType:   "trino",
		IsDenyAllElse: false,
	}
	return postPolicy(policy, "CreateTrinoTableViewerPolicy")
}
