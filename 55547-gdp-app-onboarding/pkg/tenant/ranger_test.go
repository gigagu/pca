package tenant

import (
	"encoding/json"
	"errors"
	"reflect"
	"testing"
)

// mockPostPolicy replaces postPolicy for testing
var mockPostPolicyFunc func(policy PolicyRequest) error

func TestCreateTableAdminPolicy_Success(t *testing.T) {
	called := false
	var gotPolicy PolicyRequest

	mockPostPolicyFunc = func(policy PolicyRequest) error {
		called = true
		gotPolicy = policy
		return nil
	}

	tenantName := "tenant1"
	schemaName := "schemaA"
	dbName := "dbX"
	adminGroups := []string{"group1", "group2"}

	err := CreateTableAdminPolicy(tenantName, schemaName, dbName, adminGroups)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if !called {
		t.Fatal("expected postPolicy to be called")
	}

	wantPolicy := PolicyRequest{
		IsEnabled:      true,
		Service:        "hive-dev-policy",
		Name:           "admin-tenant1-schemaA-tables",
		PolicyType:     0,
		PolicyPriority: 0,
		Description:    "Policy for all - database",
		IsAuditEnabled: true,
		Resources: map[string]PolicyResource{
			"database": {
				Values:      []string{"dbX"},
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
				DelegateAdmin: true,
			},
			{
				Accesses: []PolicyAccess{
					{"create", true},
				},
				Groups:        adminGroups,
				DelegateAdmin: false,
			},
		},
		ServiceType:   "hive",
		IsDenyAllElse: false,
	}

	// Compare JSON to avoid issues with slice ordering
	gotJSON, _ := json.Marshal(gotPolicy)
	wantJSON, _ := json.Marshal(wantPolicy)
	if !reflect.DeepEqual(gotJSON, wantJSON) {
		t.Errorf("policy mismatch\nGot:  %s\nWant: %s", gotJSON, wantJSON)
	}
}

func TestCreateTableAdminPolicy_PostPolicyError(t *testing.T) {
	mockPostPolicyFunc = func(policy PolicyRequest) error {
		return errors.New("post failed")
	}
	err := CreateTableAdminPolicy("t", "s", "d", []string{"g"})
	if err == nil || err.Error() != "post failed" {
		t.Errorf("expected error 'post failed', got %v", err)
	}
}
