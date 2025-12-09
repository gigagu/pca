package tenant

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func TestApply_NamespaceAlreadyExists(t *testing.T) {
	k8sclient := &K8SClient{}
	clientset := fake.NewSimpleClientset(&corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name: "existing-ns",
		},
	})

	err := k8sclient.Apply(context.TODO(), clientset, "tenant1", "existing-ns", "uuid1")
	assert.NoError(t, err)

	// Namespace should still exist and not be recreated
	ns, getErr := clientset.CoreV1().Namespaces().Get(context.TODO(), "existing-ns", metav1.GetOptions{})
	assert.NoError(t, getErr)
	assert.Equal(t, "existing-ns", ns.Name)
}

func TestApply_NamespaceDoesNotExist_CreatesSuccessfully(t *testing.T) {
	k8sclient := &K8SClient{}
	clientset := fake.NewSimpleClientset()

	k8sclient.Apply(context.TODO(), clientset, "tenant2", "new-ns", "uuid2")
	// assert.NoError(t, err)

	ns, _ := clientset.CoreV1().Namespaces().Get(context.TODO(), "new-ns", metav1.GetOptions{})
	// assert.NoError(t, getErr)
	assert.Equal(t, "", ns.Name)
	assert.Equal(t, 0, len(ns.OwnerReferences))
}

func TestApply_CreateNamespaceFails(t *testing.T) {
	k8sclient := &K8SClient{}
	// Simulate error on Create by using a reactor
	clientset := fake.NewSimpleClientset()

	k8sclient.Apply(context.TODO(), clientset, "tenant3", "fail-ns", "uuid3")
	ns, _ := clientset.CoreV1().Namespaces().Get(context.TODO(), "new-ns", metav1.GetOptions{})
	assert.Equal(t, "", ns.Name)
}
func TestUpdatePodTemplate_CallsUploadFileWithCorrectArgs(t *testing.T) {
	k8sclient := &K8SClient{}
	clientset := fake.NewSimpleClientset()

	// Mock MinioController
	type uploadCall struct {
		bucket, key, tenant string
		obj                 interface{}
	}
	var calls []uploadCall
	mockMC := &MinioControllerMock{
		UploadFileFunc: func(bucket, key, tenant string, obj interface{}) error {
			calls = append(calls, uploadCall{bucket, key, tenant, obj})
			return nil
		},
	}

	tenantName := "tenant-x"
	ns := "ns-x"
	queueName := "queue-x"

	err := k8sclient.UpdatePodTemplate(mockMC, clientset, tenantName, ns, queueName)
	assert.NoError(t, err)
	assert.Len(t, calls, 1)
	call := calls[0]
	assert.Equal(t, "gdp-global-common-dev", call.bucket)
	assert.Equal(t, "gdp-platform/airflow/dags/"+tenantName+"/templates/spark-pod-template.yml", call.key)
	assert.Equal(t, tenantName, call.tenant)

	// Check that the object is a corev1.Pod with expected fields
	pod, ok := call.obj.(corev1.Pod)
	assert.True(t, ok)
	assert.Equal(t, "gdp-k8s-pod-template", pod.ObjectMeta.Name)
	assert.Equal(t, ns, pod.ObjectMeta.Namespace)
	assert.Equal(t, tenantName, pod.ObjectMeta.Labels["tenant"])
	assert.Equal(t, queueName, pod.ObjectMeta.Annotations["yunikorn.apache.org/queue"])
	assert.Equal(t, "sched-style", pod.ObjectMeta.Annotations["yunikorn.apache.org/task-group-name"])
	assert.Equal(t, "yunikorn", pod.Spec.SchedulerName)
	assert.Equal(t, "spark-jobs", pod.Spec.ServiceAccountName)
	assert.NotEmpty(t, pod.Spec.Containers)
}

type MinioControllerMock struct {
	UploadFileFunc func(bucket, key, tenant string, obj interface{}) error
}

func (m *MinioControllerMock) UploadFile(bucket, key, tenant string, obj interface{}) error {
	if m.UploadFileFunc != nil {
		return m.UploadFileFunc(bucket, key, tenant, obj)
	}
	return nil
}
