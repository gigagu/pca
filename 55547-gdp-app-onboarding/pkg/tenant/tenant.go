package tenant

import (
	"TTOQPR/gdp-tenant/pkg/constants"
	gdptenantv1 "TTOQPR/gdp-tenant/pkg/crd/api/v1"
	"TTOQPR/gdp-tenant/pkg/k8s"
	"TTOQPR/gdp-tenant/pkg/model"
	"fmt"
	"net/http"
	"sync"

	"encoding/json"

	"github.com/labstack/echo/v4"
	"gopkg.in/yaml.v2"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/klog"
)

type TenantController struct {
	cf *k8s.ClientFactory
}

// new tenant
func NewTenantController(cf *k8s.ClientFactory) (*TenantController, error) {
	return &TenantController{
		cf: cf,
	}, nil
}

type TenantServiceInterface interface {
	CreateTenant(c echo.Context) error
	GetContainerMFByTenantName(tenantName string) (*model.AppManifest, error)
}

func BuildTenant(tenantName string, pc model.PlatformConfig) *gdptenantv1.GDPTenant {
	tenant := &gdptenantv1.GDPTenant{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "gdp.standardchartered.com/v1",
			Kind:       "GDPTenant",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      tenantName,
			Namespace: constants.GDPNamespace,
		},
		Spec: gdptenantv1.GDPTenantSpec{
			Tenant:                     tenantName,
			Namespace:                  pc.Namespace,
			Target:                     pc.Target,
			BC:                         pc.BC,
			ItamName:                   pc.ItamName,
			Itam:                       pc.Itam,
			TenantNoninteractiveOwner:  pc.TenantNoninteractiveOwner,
			TenantNoninteractiveViewer: pc.TenantNoninteractiveViewer,
			TenantInteractiveOwner:     pc.TenantInteractiveOwner,
			TenantInteractiveViewer:    pc.TenantInteractiveViewer,
			JobQueue:                   gdptenantv1.JobQueue{},
			ResourceQuotas: gdptenantv1.ResourceQuotas{
				CPU:    pc.ResourceQuotas.CPU,
				Memory: pc.ResourceQuotas.Memory,
				// LimitsCPU:      pc.ResourceQuotas.LimitsCPU,
				// LimitsMemory:   pc.ResourceQuotas.LimitsMemory,
			},
			ObjectStoreBuckets: []gdptenantv1.ObjectStoreBuckets{},
		},
	}
	// for _, yq := range pc.YunikornQueue {
	tenant.Spec.JobQueue = gdptenantv1.JobQueue{
		// Name:           pc.YunikornQueue.Name,
		CPU:    pc.JobQueue.CPU,
		Memory: pc.JobQueue.Memory,
		// LimitCPU:       pc.JobQueue.LimitCPU,
		// LimitMemory:    pc.JobQueue.LimitMemory,
	}

	for _, ob := range pc.ObjectStoreBuckets {
		tenant.Spec.ObjectStoreBuckets = append(tenant.Spec.ObjectStoreBuckets, gdptenantv1.ObjectStoreBuckets{
			Name:       ob.Name,
			BucketSize: ob.BucketSize,
			Location:   ob.Location,
		})
	}
	return tenant
}

func (ts *TenantController) CreateTenant(c echo.Context) error {

	am := new(model.AppManifest)
	if err := c.Bind(am); err != nil {
		if ute, ok := err.(*json.UnmarshalTypeError); ok {
			return c.String(http.StatusBadRequest,
				"field wrong: field '"+ute.Field+"' ,expect filed  "+ute.Type.String())
		}
		// check is it SyntaxError
		if se, ok := err.(*json.SyntaxError); ok {
			return c.String(http.StatusBadRequest,
				"JSON syntax error, offset: "+fmt.Sprint(se.Offset))
		}
		klog.Errorf("failed to bind request body to AppManifest: %v", err)
		return c.JSON(http.StatusBadRequest, fmt.Errorf("failed to bind request body to AppManifest: %w", err))
	}

	err := ts.ApplyTenant(am, c)
	if err != nil {
		klog.Errorf("failed to apply tenant queue: %v", err)
		return err
	}
	if len(am.MetaData.ManifestSchema) == 0 {
		return c.JSON(http.StatusInternalServerError, fmt.Errorf("tenant is empty"))
	}

	return c.String(http.StatusOK, `"message": "ok"`)
}

func (ts *TenantController) ApplyTenant(am *model.AppManifest, c echo.Context) error {
	clientset, err := ts.cf.GetLocalClient()
	if err != nil {
		return c.JSON(http.StatusInternalServerError, fmt.Errorf("failed to get local client: %w", err))
	}

	k8sclient := &K8SClient{}
	rangerClient := &Ranger{}
	minioEndpoint, accesskey, secretkey := ts.cf.GetMinioCredential()
	mc := NewMinioController(minioEndpoint, accesskey, secretkey, true)
	klog.Infof("minio client: %+v", mc)
	if mc == nil {
		klog.Errorf("failed to create minio client")
	}
	trinoClient := &Trino{}
	var tenantsInfo []map[string]interface{}

	// 用于并发
	var wg sync.WaitGroup
	errCh := make(chan error, len(am.PlatformConfig)*5) // 预估最大并发数
	var tenantsInfoMu sync.Mutex

	for _, pc := range am.PlatformConfig {
		wg.Add(1)
		go func(pc model.PlatformConfig) {
			defer wg.Done()
			// todo: convert to lower case
			tenantName := fmt.Sprintf("t-%d-%s-%s", pc.Itam, pc.Tenant, pc.TenantShortName)
			tenant := BuildTenant(tenantName, pc)

			if err := ts.cf.GDPManager.ApplyTenant(c.Request().Context(), tenant); err != nil {
				// errCh <- fmt.Errorf("failed to create tenant: %w", err)
				klog.Errorf("failed to create tenant: %v", err)
				// return
			}
			tenant, err := ts.cf.GDPManager.GetTenant(c.Request().Context(), tenantName)
			if err != nil {
				klog.Errorf("failed to get tenant: %v", err)
				// return
			}

			ns := fmt.Sprintf("%s-%s", tenantName, pc.Namespace)
			// queueName := fmt.Sprintf("t-%d-%s-%s", pc.Itam, pc.Tenant, pc.YunikornQueue.Name)
			err = k8sclient.Apply(c.Request().Context(), mc, clientset, tenantName, ns, tenant.UID, pc.ResourceQuotas.CPU, pc.ResourceQuotas.Memory, pc.SecretPath)
			if err != nil {
				// errCh <- fmt.Errorf("failed to apply k8s resources: %w", err)
				klog.Errorf("failed to apply k8s resources: %v", err)
				// return
			}

			cm, err := clientset.CoreV1().ConfigMaps(constants.YunikornNamespace).Get(c.Request().Context(), constants.YuniKornConfigMapName, metav1.GetOptions{})
			if err != nil {
				klog.Errorf("failed to get yunikorn configmap: %v", err)
				// return
			}
			yunikornConfig, err := LoadConfigMaps(cm)
			if err != nil {
				klog.Errorf("failed to load yunikorn configmap: %v", err)
				// return
			}

			var applyConfig *SchedulerConfig = yunikornConfig
			applyConfig = ApplyTenantQueue(applyConfig, 0, tenantName, pc.JobQueue.CPU, pc.JobQueue.Memory)
			data, err := yaml.Marshal(applyConfig)
			if err != nil {
				klog.Errorf("marshal applyConfig to yaml error: %v", err)
			} else {
				cm.Data["queues.yaml"] = string(data)
			}

			if _, err = clientset.CoreV1().ConfigMaps(constants.YunikornNamespace).Update(c.Request().Context(), cm, metav1.UpdateOptions{}); err != nil {
				klog.Errorf("failed to update yunikorn configmap: %v", err)
				// return
			}

			// create minio bucket
			for _, bucket := range pc.ObjectStoreBuckets {
				klog.Infof("create minio bucket: %s", bucket.Name)
				if !mc.CheckBucketExist(bucket.Name) {
					if err := mc.CreateBucket(bucket.Name, bucket.Location); err != nil {
						klog.Errorf("failed to create minio bucket %s: %v", bucket.Name, err)
						// 可以选择汇总错误，也可以只打印
						// errCh <- fmt.Errorf("failed to create minio bucket %s: %w", bucket.Name, err)
					}
				}
			}

			// trino configmap
			if err := trinoClient.Apply(c.Request().Context(), clientset, tenantName, pc.TenantNoninteractiveOwner, pc.TenantInteractiveOwner); err != nil {
				// errCh <- fmt.Errorf("failed to apply trino: %w", err)
				klog.Errorf("failed to apply trino: %v", err)
				// return
			}

			klog.Info("update range policy")
			// update ranger policy
			for _, rp := range pc.RangerPolicies {
				schemaName := rp.SchemaName
				schemaPath := rp.SchemaPath
				dbName := rp.SchemaName
				adminGroups := rp.SchemaAdmin
				viewerGroups := rp.SchemaViewer
				if err := rangerClient.Apply(tenantName, schemaName, schemaPath, dbName, adminGroups, viewerGroups); err != nil {
					// errCh <- fmt.Errorf("failed to apply ranger policy: %w", err)
					klog.Errorf("failed to apply ranger policy: %v", err)
					// return
				}
			}
			klog.Info("patch tenant status")

			status := gdptenantv1.GDPTenantStatus{
				Namespace: ns,
				Yunikorn:  tenantName,
			}
			if err := ts.cf.GDPManager.PatchTenantStatus(c.Request().Context(), tenantName, status); err != nil {
				klog.Errorf("failed to patch tenant status: %v", err)
			}

			tenantsInfoMu.Lock()
			tenantsInfo = append(tenantsInfo, map[string]interface{}{
				"tenantName":    tenantName,
				"namespace":     ns,
				"minioBuckets":  pc.ObjectStoreBuckets,
				"yunikornQueue": tenantName,
			})
			tenantsInfoMu.Unlock()
			klog.Info("tenant created successfully, response ", tenantsInfo)
		}(pc)
	}

	wg.Wait()
	close(errCh)

	var allErrs []error
	for err := range errCh {
		if err != nil {
			allErrs = append(allErrs, err)
		}
	}

	if len(allErrs) > 0 {
		return c.JSON(http.StatusInternalServerError, allErrs)
	}

	// generate reply message
	return c.JSON(http.StatusOK, tenantsInfo)
}

/**
func (ts *TenantController) ApplyTenant(am *model.AppManifest, c echo.Context) error {
	clientset, err := ts.cf.GetLocalClient()
	if err != nil {
		return c.JSON(http.StatusInternalServerError, fmt.Errorf("failed to get local client: %w", err))
	}

	k8sclient := &K8SClient{}
	rangerClient := &Ranger{}
	minioEndpoint, accesskey, secretkey := ts.cf.GetMinioCredential()

	mc := NewMinioController(minioEndpoint, accesskey, secretkey, true)
	klog.Infof("minio client: %+v", mc)
	if mc == nil {
		return c.JSON(http.StatusInternalServerError, fmt.Errorf("failed to create minio client"))
	}
	trinoClient := &Trino{}
	var tenantsInfo []map[string]interface{}
	for _, pc := range am.PlatformConfig {
		tenantName := fmt.Sprintf("t-%d-%s", pc.Itam, pc.Tenant)
		tenant := BuildTenant(tenantName, pc)

		err := ts.cf.GDPManager.ApplyTenant(c.Request().Context(), tenant)
		if err != nil {
			klog.Errorf("create tenant response %v", err)
			return c.JSON(http.StatusInternalServerError, fmt.Errorf("failed to create tenant: %w", err))

		}
		tenant, err = ts.cf.GDPManager.GetTenant(c.Request().Context(), tenantName)
		if err != nil {
			klog.Errorf("get tenant response %v", err)
			return c.JSON(http.StatusInternalServerError, fmt.Errorf("failed to get tenant: %w", err))
		}

		ns := fmt.Sprintf("%s-%s", tenantName, pc.Namespace)
		queueName := fmt.Sprintf("t-%d-%s", pc.Itam, pc.YunikornQueue.Name)
		k8sclient.Apply(c.Request().Context(), mc, clientset, tenantName, ns, queueName, tenant.UID)

		cm, err := clientset.CoreV1().ConfigMaps(constants.YunikornNamespace).Get(c.Request().Context(), constants.YuniKornConfigMapName, metav1.GetOptions{})
		if err != nil {
			klog.Errorf("failed to get yunikorn configmap: %v", err)
		}
		yunikornConfig, err := LoadConfigMaps(cm)
		if err != nil {
			klog.Errorf("failed to load yunikorn configmap: %v", err)
		}

		var applyConfig *SchedulerConfig = yunikornConfig
		applyConfig = ApplyTenantQueue(applyConfig, 0, queueName, pc.YunikornQueue.RequestsMemory, pc.ResourceQuotas.RequestsCPU, pc.ResourceQuotas.LimitsCPU, pc.ResourceQuotas.LimitsMemory)
		data, err := yaml.Marshal(applyConfig)
		if err != nil {
			klog.Errorf("marshal applyConfig to yaml error: %v", err)
		} else {
			cm.Data["queues.yaml"] = string(data)
		}

		_, err = clientset.CoreV1().ConfigMaps(constants.YunikornNamespace).Update(c.Request().Context(), cm, metav1.UpdateOptions{})
		if err != nil {
			return c.JSON(http.StatusInternalServerError, fmt.Errorf("failed to update yunikorn configmap: %w", err))
		}

		// create minio bucket
		for _, bucket := range pc.ObjectStoreBuckets {
			klog.Infof("create minio bucket: %s", bucket.Name)
			if !mc.CheckBucketExist(bucket.Name) {
				err = mc.CreateBucket(bucket.Name, bucket.Location)
				if err != nil {
					klog.Errorf("failed to create minio bucket %s: %v", bucket.Name, err)
					// annotate because minio do not give us create bucket permission
					// return c.JSON(http.StatusInternalServerError, fmt.Errorf("failed to create minio bucket %s: %w", bucket.Name, err))
				}
			}
		}

		// trino configmap
		err = trinoClient.Apply(c.Request().Context(), clientset, tenantName, pc.TenantNoninteractiveOwner, pc.TenantInteractiveOwner)
		if err != nil {
			klog.Errorf("failed to apply trino: %v", err)
			return c.JSON(http.StatusInternalServerError, fmt.Errorf("failed to apply trino: %w", err))
		}

		klog.Info("update range policy")
		// update ranger policy
		for _, rp := range pc.RangerPolicies {
			schemaName := rp.SchemaName
			schemaPath := rp.SchemaPath
			dbName := rp.SchemaName
			adminGroups := rp.SchemaAdmin
			viewerGroups := rp.SchemaViewer
			err = rangerClient.Apply(tenantName, schemaName, schemaPath, dbName, adminGroups, viewerGroups)
			if err != nil {
				klog.Errorf("failed to apply ranger policy: %v", err)
				return c.JSON(http.StatusInternalServerError, fmt.Errorf("failed to apply ranger policy: %w", err))
			}
		}
		klog.Info("patch tenant status")

		status := gdptenantv1.GDPTenantStatus{
			Namespace: ns,
			Yunikorn:  queueName,
		}
		err = ts.cf.GDPManager.PatchTenantStatus(c.Request().Context(), tenantName, status)
		if err != nil {
			klog.Errorf("failed to patch tenant status: %v", err)
		}

		tenantsInfo = append(tenantsInfo, map[string]interface{}{
			"tenantName":    tenantName,
			"namespace":     ns,
			"minioBuckets":  pc.ObjectStoreBuckets,
			"yunikornQueue": queueName,
		})
		klog.Info("tenant created successfully, response ", tenantsInfo)

	}

	// generate reply message
	return c.JSON(http.StatusOK, tenantsInfo)

}
*/

func (ts *TenantController) GetContainerMFByTenantName(tenantName string) (*model.AppManifest, error) {
	// update yunikorn queue

	// create minio bucket

	// create SKE namespace

	// Create kafka topic

	// update trino configmap

	// update ranger

	// update datahub

	return nil, nil
}
