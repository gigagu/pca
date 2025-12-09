package tenant

import (
	"context"
	"fmt"
	"strings"

	// "gopkg.in/yaml.v2"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/klog"
)

type K8SClient struct{}

func resourceMustParse(val string) resource.Quantity {
	q, _ := resource.ParseQuantity(val)
	return q
}

func (k8sclient *K8SClient) Apply(ctx context.Context, mc *MinioController, clientset kubernetes.Interface, tenantName string, ns string, uuid types.UID, cpu string, memory string, secretPath string) error {
	_, err := clientset.CoreV1().Namespaces().Get(ctx, ns, metav1.GetOptions{})
	ownerRef := metav1.OwnerReference{
		APIVersion: "gdp.standardchartered.com/v1",
		Kind:       "GDPTenant",
		Name:       tenantName,
		UID:        uuid,
	}

	_, createErr := clientset.CoreV1().Namespaces().Create(ctx, &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name:            ns,
			OwnerReferences: []metav1.OwnerReference{ownerRef},
		},
	}, metav1.CreateOptions{})
	if createErr != nil {
		klog.Errorf("failed to create namespace %s: %v", ns, createErr)
	}

	role := &rbacv1.Role{
		ObjectMeta: metav1.ObjectMeta{
			Name:            fmt.Sprintf("%s-view", tenantName),
			OwnerReferences: []metav1.OwnerReference{ownerRef},
		},
		Rules: []rbacv1.PolicyRule{
			{
				APIGroups: []string{""},
				Resources: []string{"pods"},
				Verbs:     []string{"get", "list", "watch"},
			},
			{
				APIGroups: []string{""},
				Resources: []string{"pods/log"},
				Verbs:     []string{"get", "list", "watch"},
			},
			{
				APIGroups: []string{"batch"},
				Resources: []string{"jobs"},
				Verbs:     []string{"get", "list"},
			},
			{
				APIGroups: []string{"events.k8s.io"},
				Resources: []string{"events"},
				Verbs:     []string{"get", "list", "watch"},
			},
		},
	}

	_, createErr = clientset.RbacV1().Roles(ns).Create(ctx, role, metav1.CreateOptions{})
	if createErr != nil {
		clientset.RbacV1().Roles(ns).Update(ctx, role, metav1.UpdateOptions{})

		klog.Errorf("failed to create role %s: %v", fmt.Sprintf("%s-view", tenantName), createErr)
	}

	groupname := fmt.Sprintf("suz1-apps-gdp-%s-view", strings.TrimPrefix(tenantName, "t-"))
	rolebinding := &rbacv1.RoleBinding{
		ObjectMeta: metav1.ObjectMeta{
			Name:            fmt.Sprintf("%s-view", tenantName),
			OwnerReferences: []metav1.OwnerReference{ownerRef},
		},
		RoleRef: rbacv1.RoleRef{
			APIGroup: "rbac.authorization.k8s.io",
			Kind:     "Role",
			Name:     fmt.Sprintf("%s-view", tenantName),
		},
		Subjects: []rbacv1.Subject{
			{
				Kind: "Group",
				// our pattern will be suz1-apps-gdp-ITAM-Tenant-ShortName-view
				Name: groupname,
			},
		},
	}
	if createErr != nil {
		klog.Errorf("failed to create namespace %s: %v", ns, createErr)

	}

	_, createErr = clientset.RbacV1().RoleBindings(ns).Create(ctx, rolebinding, metav1.CreateOptions{})
	if createErr != nil {
		klog.Errorf("failed to create rolebinding %s: %v", fmt.Sprintf("%s-view", tenantName), createErr)
		clientset.RbacV1().RoleBindings(ns).Update(ctx, rolebinding, metav1.UpdateOptions{})
	}

	// ResourceQuota
	resourceQuota := &corev1.ResourceQuota{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "gdp-tenant-quota",
			Namespace: ns,
		},
		Spec: corev1.ResourceQuotaSpec{
			Hard: corev1.ResourceList{
				corev1.ResourceCPU:    resourceMustParse(cpu),
				corev1.ResourceMemory: resourceMustParse(memory),
			},
		},
	}
	_, rqErr := clientset.CoreV1().ResourceQuotas(ns).Create(ctx, resourceQuota, metav1.CreateOptions{})
	if rqErr != nil {
		clientset.CoreV1().ResourceQuotas(ns).Update(ctx, resourceQuota, metav1.UpdateOptions{})
		klog.Errorf("failed to create ResourceQuota in namespace %s: %v", ns, rqErr)
	}

	//Role
	role = &rbacv1.Role{
		ObjectMeta: metav1.ObjectMeta{
			Name:            "spark-job-role",
			OwnerReferences: []metav1.OwnerReference{ownerRef},
			Namespace:       ns,
		},
		Rules: []rbacv1.PolicyRule{
			{
				APIGroups: []string{""},
				Resources: []string{"pods", "pods/exec"},
				Verbs:     []string{"create", "get", "list", "watch", "delete"},
			},
			{
				APIGroups: []string{""},
				Resources: []string{"pods/log"},
				Verbs:     []string{"get", "list", "watch"},
			},
			{
				APIGroups: []string{""},
				Resources: []string{"configmaps"},
				Verbs:     []string{"create", "delete", "get", "list", "watch"},
			},
			{
				APIGroups: []string{""},
				Resources: []string{"persistentvolumeclaims"},
				Verbs:     []string{"create", "get", "list", "watch", "delete"},
			},
		},
	}
	_, err = clientset.RbacV1().Roles(ns).Create(ctx, role, metav1.CreateOptions{})
	if err != nil {
		clientset.RbacV1().Roles(ns).Update(ctx, role, metav1.UpdateOptions{})
		klog.Errorf("failed to create Role spark-job-role in namespace %s: %v", ns, err)
	}

	// RoleBinding
	roleBinding := &rbacv1.RoleBinding{
		ObjectMeta: metav1.ObjectMeta{
			Name:            "sparkjob-binding",
			OwnerReferences: []metav1.OwnerReference{ownerRef},
			Namespace:       ns,
		},
		Subjects: []rbacv1.Subject{
			{
				Kind: "ServiceAccount",
				Name: "vault-auth",
				// Name:      "spark-jobs-sa",
				Namespace: ns,
			},
			{
				Kind:      "ServiceAccount",
				Name:      "gdp-airflow-worker",
				Namespace: "gdp-system",
			},
			{
				Kind:      "ServiceAccount",
				Name:      "gdp-airflow-webserver",
				Namespace: "gdp-system",
			},
		},
		RoleRef: rbacv1.RoleRef{
			Kind:     "Role",
			Name:     "spark-job-role",
			APIGroup: "rbac.authorization.k8s.io",
		},
	}
	_, err = clientset.RbacV1().RoleBindings(ns).Create(ctx, roleBinding, metav1.CreateOptions{})
	if err != nil {
		clientset.RbacV1().RoleBindings(ns).Update(ctx, roleBinding, metav1.UpdateOptions{})
		klog.Errorf("failed to create RoleBinding sparkjob-binding in namespace %s: %v", ns, err)
	}

	klog.Infof("RoleBinding %s created", ns)

	k8sclient.UpdatePodTemplate(mc, clientset, tenantName, ns, secretPath)

	return nil
}

func (k8sclient *K8SClient) UpdatePodTemplate(mc *MinioController, clientset kubernetes.Interface, tenantName string, ns string, secretPath string) error {
	// var roleName string = "55547_global_app_k8s_ _role"
	var roleName string = fmt.Sprintf("55547_global_app_k8s_%s_role", ns)
	var PodTemplate = corev1.Pod{
		TypeMeta: metav1.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "gdp-k8s-pod-template",
			Namespace: ns,
			Labels: map[string]string{
				"tenant": tenantName,
			},

			Annotations: map[string]string{
				"yunikorn.apache.org/queue":           tenantName,
				"yunikorn.apache.org/task-group-name": "sched-style",

				"vault.hashicorp.com/agent-inject":                             "true",
				"vault.hashicorp.com/agent-pre-populate-only":                  "true",
				"vault.hashicorp.com/agent-set-security-context":               "true",
				"vault.hashicorp.com/agent-run-as-user":                        "1002",
				"vault.hashicorp.com/agent-run-as-group":                       "1002",
				"vault.hashicorp.com/agent-inject-token":                       "true",
				"vault.hashicorp.com/agent-init-json-patch":                    "[{\"op\": \"replace\", \"path\": \"/securityContext/seccompProfile\", \"value\": {\"type\": \"RuntimeDefault\"}}]",
				"vault.hashicorp.com/agent-json-patch":                         "[{\"op\": \"replace\", \"path\": \"/securityContext/seccompProfile\", \"value\": {\"type\": \"RuntimeDefault\"}}]",
				"vault.hashicorp.com/agent-service-account-token-volume-name":  "vault-auth",
				"vault.hashicorp.com/role":                                     roleName,
				"vault.hashicorp.com/agent-inject-secret-s3-authorizer-secret": secretPath,
				"vault.hashicorp.com/agent-inject-template-s3-authorizer-secret": fmt.Sprintf(`
					{{- with secret "%s" -}}
					{{ .Data.username }}
					{{ .Data.password }}
					{{- end }}`, secretPath),
				"vault.hashicorp.com/secret-volume-path": "/var/run/secret/auth/",
			},
		},
		Spec: corev1.PodSpec{
			AutomountServiceAccountToken: func(b bool) *bool { return &b }(false),
			SchedulerName:                "yunikorn",
			SecurityContext: &corev1.PodSecurityContext{
				RunAsUser:          func(i int64) *int64 { v := int64(10000); return &v }(10000),
				RunAsGroup:         func(i int64) *int64 { v := int64(10000); return &v }(10000),
				SupplementalGroups: []int64{10000},
			},
			// ServiceAccountName: "spark-jobs",
			ServiceAccountName: "vault-auth",
			Containers: []corev1.Container{
				{
					Name:            "gdp-k8s-pod-template",
					ImagePullPolicy: corev1.PullAlways,
					SecurityContext: &corev1.SecurityContext{
						RunAsNonRoot:             func(b bool) *bool { return &b }(true),
						AllowPrivilegeEscalation: func(b bool) *bool { return &b }(false),
						Capabilities: &corev1.Capabilities{
							Drop: []corev1.Capability{"ALL"},
						},
						SeccompProfile: &corev1.SeccompProfile{
							Type: corev1.SeccompProfileTypeRuntimeDefault,
						},
					},
					Env: []corev1.EnvVar{
						{
							Name: "POD_NAME",
							ValueFrom: &corev1.EnvVarSource{
								FieldRef: &corev1.ObjectFieldSelector{
									FieldPath: "metadata.name",
								},
							},
						},
						{Name: "S3_AUTHORIZER_AUTH_FILE", Value: "/var/run/secret/auth/s3-authorizer-secret"},
						{Name: "HIVE_ENDPOINT_URL", Value: "thrift://gdp-hive-metastore.gdp-system.svc.cluster.local:9083"},
						{Name: "S3_AUTHORIZER_SERVER_BASE_URL", Value: "https://s3authorizer-prd.sked011.55547.app.standardchartered.com"},
						{Name: "S3_ENDPOINT_URL", Value: "https://minio-14-prd.50821.app.standardchartered.com:4111"},
						{Name: "AWS_ACCESS_KEY", Value: "NO_NEED"},
						{Name: "AWS_SECRET_KEY", Value: "NO_NEED"},
						{
							Name: "DATAHUB_REST_TOKEN",
							ValueFrom: &corev1.EnvVarSource{
								SecretKeyRef: &corev1.SecretKeySelector{
									LocalObjectReference: corev1.LocalObjectReference{Name: "datahub-credential"},
									Key:                  "DATAHUB_REST_TOKEN",
								},
							},
						},
						{
							Name: "DATAHUB_REST_ENDPOINT",
							ValueFrom: &corev1.EnvVarSource{
								SecretKeyRef: &corev1.SecretKeySelector{
									LocalObjectReference: corev1.LocalObjectReference{Name: "datahub-credential"},
									Key:                  "DATAHUB_REST_ENDPOINT",
								},
							},
						},
					},
					VolumeMounts: []corev1.VolumeMount{
						{Name: "gdp-truststore-volume", MountPath: "/etc/gdp/ssl", ReadOnly: true},
						// {Name: "gdp-tenant-profile-volume", MountPath: "/etc/gdp/profiles/default", ReadOnly: true},
						{Name: "k8s-sa-token", MountPath: "/var/run/secrets/kubernetes.io/serviceaccount", ReadOnly: true},
						{Name: "s3auth-client-certs-vol", MountPath: "/var/run/secrets/s3authorizer"},
					},
					Resources: corev1.ResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceMemory: resourceMustParse("1Gi"),
							corev1.ResourceCPU:    resourceMustParse("1"),
						},
						Limits: corev1.ResourceList{
							corev1.ResourceMemory: resourceMustParse("2Gi"),
							corev1.ResourceCPU:    resourceMustParse("1"),
						},
					},
				},
			},
			Volumes: []corev1.Volume{
				{
					Name: "vault-auth",
					VolumeSource: corev1.VolumeSource{
						Projected: &corev1.ProjectedVolumeSource{
							Sources: []corev1.VolumeProjection{
								{
									ServiceAccountToken: &corev1.ServiceAccountTokenProjection{
										Path:              "vault-auth",
										ExpirationSeconds: func(i int64) *int64 { v := int64(7200); return &v }(7200),
										Audience:          "vault",
									},
								},
							},
						},
					},
				},
				{
					Name: "gdp-truststore-volume",
					VolumeSource: corev1.VolumeSource{
						Secret: &corev1.SecretVolumeSource{
							SecretName: "gdp-truststore-bundle",
						},
					},
				},
				// {
				// 	Name: "gdp-tenant-profile-volume",
				// 	VolumeSource: corev1.VolumeSource{
				// 		ConfigMap: &corev1.ConfigMapVolumeSource{
				// 			LocalObjectReference: corev1.LocalObjectReference{Name: "gdp-tenant-profile"},
				// 		},
				// 	},
				// },
				{
					Name: "k8s-sa-token",
					VolumeSource: corev1.VolumeSource{
						Projected: &corev1.ProjectedVolumeSource{
							DefaultMode: func(i int32) *int32 { v := int32(420); return &v }(420),
							Sources: []corev1.VolumeProjection{
								{
									ServiceAccountToken: &corev1.ServiceAccountTokenProjection{
										Audience:          "https://kubernetes.default.svc.cluster.local",
										ExpirationSeconds: func(i int64) *int64 { v := int64(7200); return &v }(7200),
										Path:              "token",
									},
								},
								{
									ConfigMap: &corev1.ConfigMapProjection{
										LocalObjectReference: corev1.LocalObjectReference{Name: "kube-root-ca.crt"},
										Items: []corev1.KeyToPath{
											{Key: "ca.crt", Path: "ca.crt"},
										},
									},
								},
								{
									DownwardAPI: &corev1.DownwardAPIProjection{
										Items: []corev1.DownwardAPIVolumeFile{
											{
												FieldRef: &corev1.ObjectFieldSelector{
													APIVersion: "v1",
													FieldPath:  "metadata.namespace",
												},
												Path: "namespace",
											},
										},
									},
								},
							},
						},
					},
				},
				// {
				// 	Name: "s3auth-client-certs-vol",
				// 	VolumeSource: corev1.VolumeSource{
				// 		Projected: &corev1.ProjectedVolumeSource{
				// 			Sources: []corev1.VolumeProjection{
				// 				{
				// 					Secret: &corev1.SecretProjection{
				// 						LocalObjectReference: corev1.LocalObjectReference{Name: "spark-jobs-s3authorizer-secret"},
				// 						Items: []corev1.KeyToPath{
				// 							{Key: "ca.crt", Path: "ca.crt"},
				// 							{Key: "client.crt", Path: "client.crt"},
				// 							{Key: "client.pem", Path: "client.pem"},
				// 						},
				// 					},
				// 				},
				// 			},
				// 		},
				// 	},
				// },
			},
		}}

	// upload to minio
	// s3://gdp-global-common-dev/gdp-platform/airflow/dags/t-55547-demo/templates/spark-pod-template.yml
	// mc.UploadFile("gdp-global-common-dev", "gdp-platform/airflow/dags/"+tenantName+"/templates/spark-pod-template.yml", tenantName, PodTemplate)
	mc.UploadFile("sc-gdp-gbl-common-prod-55547", "gdp-platform/airflow/dags/"+tenantName+"/templates/spark-pod-template.yml", tenantName, PodTemplate)

	return nil

}
