/*
Copyright 2025.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controller

import (
	"context"
	"fmt"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	appv1 "github.com/myuser/redis-operator/api/v1"
	monitoringv1 "github.com/prometheus-operator/prometheus-operator/pkg/apis/monitoring/v1"
)

// RedisClusterReconciler reconciles a RedisCluster object
type RedisClusterReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=cache.example.com,resources=redisclusters,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=cache.example.com,resources=redisclusters/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=cache.example.com,resources=redisclusters/finalizers,verbs=update
// +kubebuilder:rbac:groups=apps,resources=statefulsets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core,resources=services,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=monitoring.coreos.com,resources=servicemonitors,verbs=get;list;watch;create;update;patch;delete

func (r *RedisClusterReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)
	logger.Info("Reconcile loop started", "request", req.NamespacedName)

	// 1. Fetch the RedisCluster instance
	cluster := &appv1.RedisCluster{}
	if err := r.Get(ctx, req.NamespacedName, cluster); err != nil {
		if errors.IsNotFound(err) {
			logger.Info("RedisCluster resource not found. Ignoring since object must be deleted.")
			return ctrl.Result{}, nil
		}
		logger.Error(err, "Failed to get RedisCluster")
		return ctrl.Result{}, err
	}

	// 2. Reconcile the Headless Service (for the StatefulSet)
	svc := r.serviceForRedisCluster(cluster)
	if err := r.reconcileService(ctx, cluster, svc); err != nil {
		logger.Error(err, "Failed to reconcile Service")
		return ctrl.Result{}, err
	}

	// 3. Reconcile the StatefulSet (with redis + exporter sidecar)
	sts := r.statefulSetForRedisCluster(cluster)
	if err := r.reconcileStatefulSet(ctx, cluster, sts); err != nil {
		logger.Error(err, "Failed to reconcile StatefulSet")
		return ctrl.Result{}, err
	}

	// 4. Reconcile the ServiceMonitor (for Prometheus)
	sm := r.serviceMonitorForRedisCluster(cluster, svc)
	if err := r.reconcileServiceMonitor(ctx, cluster, sm); err != nil {
		logger.Error(err, "Failed to reconcile ServiceMonitor")
		return ctrl.Result{}, err
	}

	// --- Your autoscaling logic would go here ---
	// You can now re-add your old logic. It will query Prometheus
	// for the metrics being exported by the sidecar we just created.
	// For example:
	// if cluster.Spec.AutoScaleEnabled {
	//     ... your autoscaling logic ...
	// }

	logger.Info("Successfully reconciled all resources")
	return ctrl.Result{Requeue: true}, nil // Requeue to check status
}

// reconcileService handles creation and updates for the Service
func (r *RedisClusterReconciler) reconcileService(ctx context.Context, cluster *appv1.RedisCluster, desired *corev1.Service) error {
	// Set RedisCluster instance as the owner and controller
	if err := controllerutil.SetControllerReference(cluster, desired, r.Scheme); err != nil {
		return err
	}
	return r.reconcileResource(ctx, desired)
}

// reconcileStatefulSet handles creation and updates for the StatefulSet
func (r *RedisClusterReconciler) reconcileStatefulSet(ctx context.Context, cluster *appv1.RedisCluster, desired *appsv1.StatefulSet) error {
	// Set RedisCluster instance as the owner and controller
	if err := controllerutil.SetControllerReference(cluster, desired, r.Scheme); err != nil {
		return err
	}
	return r.reconcileResource(ctx, desired)
}

// reconcileServiceMonitor handles creation and updates for the ServiceMonitor
func (r *RedisClusterReconciler) reconcileServiceMonitor(ctx context.Context, cluster *appv1.RedisCluster, desired *monitoringv1.ServiceMonitor) error {
	// Set RedisCluster instance as the owner and controller
	if err := controllerutil.SetControllerReference(cluster, desired, r.Scheme); err != nil {
		return err
	}
	return r.reconcileResource(ctx, desired)
}

// reconcileResource is a generic helper to create or update a resource
func (r *RedisClusterReconciler) reconcileResource(ctx context.Context, obj client.Object) error {
	key := client.ObjectKeyFromObject(obj)
	current := obj.DeepCopyObject().(client.Object)

	if err := r.Get(ctx, key, current); err != nil {
		if errors.IsNotFound(err) {
			// Create the resource
			log.FromContext(ctx).Info("Creating new resource", "Kind", obj.GetObjectKind().GroupVersionKind().Kind, "Name", key.Name)
			return r.Create(ctx, obj)
		}
		return err // Other error
	}

	// Update the resource
	// This is a simple update, for production you might need a strategic merge
	log.FromContext(ctx).Info("Updating existing resource", "Kind", obj.GetObjectKind().GroupVersionKind().Kind, "Name", key.Name)
	// We need to copy the resource version from the current object to the desired object for the update to work
	obj.SetResourceVersion(current.GetResourceVersion())
	return r.Update(ctx, obj)
}

// serviceForRedisCluster defines the desired Headless Service
func (r *RedisClusterReconciler) serviceForRedisCluster(cluster *appv1.RedisCluster) *corev1.Service {
	labels := getLabels(cluster)

	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      cluster.Name + "-headless",
			Namespace: cluster.Namespace,
			Labels:    labels,
		},
		Spec: corev1.ServiceSpec{
			ClusterIP: "None", // Headless service
			Selector:  labels,
			Ports: []corev1.ServicePort{
				{
					Name: "redis",
					Port: 6379,
				},
				{
					Name: "metrics",
					Port: 9121,
				},
			},
		},
	}
}

// statefulSetForRedisCluster defines the desired StatefulSet
func (r *RedisClusterReconciler) statefulSetForRedisCluster(cluster *appv1.RedisCluster) *appsv1.StatefulSet {
	labels := getLabels(cluster)
	replicas := cluster.Spec.Size

	return &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      cluster.Name,
			Namespace: cluster.Namespace,
			Labels:    labels,
		},
		Spec: appsv1.StatefulSetSpec{
			Replicas:    &replicas,
			ServiceName: cluster.Name + "-headless",
			Selector: &metav1.LabelSelector{
				MatchLabels: labels,
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: labels,
					Annotations: map[string]string{
						"prometheus.io/scrape": "true",
						"prometheus.io/port":   "9121",
						"prometheus.io/path":   "/metrics",
					},
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						// --- Main Redis Container ---
						{
							Name:  "redis",
							Image: fmt.Sprintf("redis:%s", cluster.Spec.RedisVersion), // Use version from CR
							Ports: []corev1.ContainerPort{
								{ContainerPort: 6379, Name: "redis"},
							},
							// TODO: Add readiness/liveness probes and volume mounts
						},
						// --- Redis Exporter Sidecar ---
						{
							Name:  "redis-exporter",
							Image: "bitnamilegacy/redis-exporter:1.59.0",
							Args: []string{
								"--redis.addr=redis://localhost:6379",
								// TODO: Add password support if needed
								// "--redis.password=$(REDIS_PASSWORD)"
							},
							// TODO: Add EnvFrom secret if password is used
							Ports: []corev1.ContainerPort{
								{ContainerPort: 9121, Name: "metrics"},
							},
							Resources: corev1.ResourceRequirements{
								Requests: corev1.ResourceList{
									corev1.ResourceCPU:    resource.MustParse("100m"),
									corev1.ResourceMemory: resource.MustParse("128Mi"),
								},
								Limits: corev1.ResourceList{
									corev1.ResourceCPU:    resource.MustParse("200m"),
									corev1.ResourceMemory: resource.MustParse("256Mi"),
								},
							},
						},
					},
				},
			},
			// TODO: Add VolumeClaimTemplates for persistent storage
		},
	}
}

// serviceMonitorForRedisCluster defines the desired ServiceMonitor
func (r *RedisClusterReconciler) serviceMonitorForRedisCluster(cluster *appv1.RedisCluster, svc *corev1.Service) *monitoringv1.ServiceMonitor {
	return &monitoringv1.ServiceMonitor{
		ObjectMeta: metav1.ObjectMeta{
			Name:      cluster.Name,
			Namespace: cluster.Namespace,
			Labels: map[string]string{
				"release": "prometheus", // This label tells the Prometheus Operator to find it
			},
		},
		Spec: monitoringv1.ServiceMonitorSpec{
			Selector: metav1.LabelSelector{
				MatchLabels: svc.Labels, // Selects the Service we created
			},
			Endpoints: []monitoringv1.Endpoint{
				{
					Port:     "metrics", // Tells Prometheus to scrape the "metrics" port (9121)
					Interval: "15s",
				},
			},
		},
	}
}

// getLabels is a helper to generate consistent labels
func getLabels(cluster *appv1.RedisCluster) map[string]string {
	return map[string]string{
		"app":       "redis-cluster",
		"cluster":   cluster.Name,
		"component": "redis",
	}
}

// SetupWithManager sets up the controller with the Manager.
func (r *RedisClusterReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&appv1.RedisCluster{}).
		Owns(&appsv1.StatefulSet{}).          // Watch StatefulSets
		Owns(&corev1.Service{}).              // Watch Services
		Owns(&monitoringv1.ServiceMonitor{}). // Watch ServiceMonitors
		Named("rediscluster").
		Complete(r)
}
