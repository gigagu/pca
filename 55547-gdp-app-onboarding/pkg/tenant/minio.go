package tenant

// create bucket
// object-store-buckets:
//   - name: "ebbs_raw_prd"
//     bucket-size: "100"
//   - name: "ebbs_curated_prd"
//     bucket-size: "100"

// new tenant onboard, create bucket
// modify tenant, update bucket

import (
	"context"
	"fmt"
	"os"

	madmin "github.com/minio/madmin-go/v4"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/minio/minio-go/v7/pkg/lifecycle"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/klog/v2"
	"sigs.k8s.io/yaml"
)

type MinioController struct {
	client      *minio.Client
	adminClient *madmin.AdminClient
	ctx         context.Context
}

func NewMinioController(endpoint, accessKey, secretKey string, useSSL bool) *MinioController {
	mc, err := minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(accessKey, secretKey, ""),
		Secure: useSSL,
	})

	if err != nil {
		klog.Fatal("New minio endpoint client error:", err)
		return nil
	}
	adminClient, err := madmin.NewWithOptions(endpoint, &madmin.Options{
		Creds:  credentials.NewStaticV4(accessKey, secretKey, ""),
		Secure: useSSL,
	})
	if err != nil {
		klog.Fatal("New minio admin client error:", err)
		return nil
	}
	return &MinioController{
		client:      mc,
		adminClient: adminClient,
		ctx:         context.Background(),
	}
}

func (mc *MinioController) CreateBucket(bucketName, location string) error {
	err := mc.client.MakeBucket(mc.ctx, bucketName, minio.MakeBucketOptions{Region: location})
	if err != nil {
		exists, errBucketExists := mc.client.BucketExists(mc.ctx, bucketName)
		if errBucketExists == nil && exists {
			klog.Infof("Bucket %s already exists\n", bucketName)
			return nil
		} else {
			return err
		}
	}
	klog.Infof("Successfully created bucket %s\n", bucketName)
	return nil
}

func (mc *MinioController) SetBucketQuota(bucketName string, quotaBytes uint64) error {
	quotaCfg := &madmin.BucketQuota{
		Type: madmin.HardQuota,
		Size: quotaBytes,
	}
	err := mc.adminClient.SetBucketQuota(mc.ctx, bucketName, quotaCfg)
	if err != nil {
		klog.Errorf("Set quota for bucket %s failed: %v", bucketName, err)
		return err
	}
	klog.Infof("Set quota for bucket %s to %d bytes", bucketName, quotaBytes)
	return nil
}

// func (mc *MinioController) ListBuckets() (minio.BucketInfo, error) {
// 	buckets, err := mc.client.ListBuckets(mc.ctx)
// 	if err != nil {
// 		klog.Fatalf("List buckets error: %v", err)
// 		return nil, err
// 	}
// 	// for _, bucket := range buckets {
// 	// 	klog.Infof(bucket.Name, bucket.CreationDate)
// 	// }
// 	return buckets, nil
// }

func (mc *MinioController) CheckBucketExist(bucketName string) bool {
	exists, err := mc.client.BucketExists(mc.ctx, bucketName)
	if err != nil {
		klog.Errorf("Check bucket %s exist error: %v", bucketName, err)
		return false
	}
	return exists
}

func (mc *MinioController) SetBucketLifecycle(bucketName string, lifecycleConfig *lifecycle.Configuration) error {
	return mc.client.SetBucketLifecycle(mc.ctx, bucketName, lifecycleConfig)
}

func (mc *MinioController) UploadFile(bucketName, objectName string, tenantName string, podTemplate corev1.Pod) error {
	podYaml, err := yaml.Marshal(podTemplate)
	var filePath string
	if err != nil {
		klog.Errorf("failed to marshal PodTemplate to yaml: %v", err)
		return fmt.Errorf("failed to marshal PodTemplate to yaml: %v", err)
	} else {
		filePath = fmt.Sprintf("./podtemplate-%s.yaml", tenantName)
		if err := os.WriteFile(filePath, podYaml, 0644); err != nil {
			klog.Errorf("failed to write PodTemplate yaml file: %v", err)
			return fmt.Errorf("failed to write PodTemplate yaml file: %v", err)
		} else {
			klog.Infof("PodTemplate yaml saved to %s", filePath)
		}
	}

	// filePath := "/tmp/" + objectName
	// filePath := fileName
	contentType := "text/plain"
	_, err = mc.client.FPutObject(mc.ctx, bucketName, objectName, filePath, minio.PutObjectOptions{
		ContentType: contentType,
	})
	if err != nil {
		klog.Errorf("Failed to upload file %s to bucket %s: %v", filePath, bucketName, err)
		return err
	}
	klog.Infof("Successfully uploaded file %s to bucket %s as object %s", filePath, bucketName, objectName)
	return nil
}

// err := mc.CreateBucket("mybucket", "us-east-1")
// if err == nil {
//     mc.SetBucketQuota("mybucket", 100*1024*1024*1024) // 100GB
// }*/
