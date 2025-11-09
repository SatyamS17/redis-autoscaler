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
	"strings"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	appv1 "github.com/myuser/redis-operator/api/v1" // Make sure this import path matches your project
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
// +kubebuilder:rbac:groups=core,resources=configmaps,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=batch,resources=jobs,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=monitoring.coreos.com,resources=servicemonitors,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch

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

	// 2. Reconcile the ConfigMap for redis.conf
	cm := r.configMapForRedisCluster(cluster)
	if err := r.reconcileConfigMap(ctx, cluster, cm); err != nil {
		logger.Error(err, "Failed to reconcile ConfigMap")
		return ctrl.Result{}, err
	}

	// 3. Reconcile the Headless Service
	svc := r.serviceForRedisCluster(cluster)
	if err := r.reconcileService(ctx, cluster, svc); err != nil {
		logger.Error(err, "Failed to reconcile Service")
		return ctrl.Result{}, err
	}

	// 4. Reconcile the StatefulSet
	sts := r.statefulSetForRedisCluster(cluster)
	if err := r.reconcileStatefulSet(ctx, cluster, sts); err != nil {
		logger.Error(err, "Failed to reconcile StatefulSet")
		return ctrl.Result{}, err
	}

	// 5. Reconcile the ServiceMonitor
	sm := r.serviceMonitorForRedisCluster(cluster, svc)
	if err := r.reconcileServiceMonitor(ctx, cluster, sm); err != nil {
		logger.Error(err, "Failed to reconcile ServiceMonitor")
		return ctrl.Result{}, err
	}

	// --- Cluster Bootstrap Logic ---

	// 6. Check if StatefulSet pods are all ready
	foundSts := &appsv1.StatefulSet{}
	if err := r.Get(ctx, client.ObjectKeyFromObject(sts), foundSts); err != nil {
		logger.Error(err, "Failed to get StatefulSet for status check")
		return ctrl.Result{}, err
	}

	totalReplicas := cluster.Spec.Masters * (1 + cluster.Spec.ReplicasPerMaster)
	if foundSts.Status.ReadyReplicas != totalReplicas {
		logger.Info("Waiting for all StatefulSet replicas to be ready", "Ready", foundSts.Status.ReadyReplicas, "Desired", totalReplicas)
		return ctrl.Result{RequeueAfter: 5 * time.Second}, nil // Requeue until all pods are ready
	}

	// 7. If all pods are ready, check if cluster is already initialized
	if !cluster.Status.Initialized {
		// 8. Cluster is not initialized, check for/run the bootstrap Job
		bootstrapJob := &batchv1.Job{}
		jobName := cluster.Name + "-bootstrap"
		err := r.Get(ctx, client.ObjectKey{Name: jobName, Namespace: cluster.Namespace}, bootstrapJob)

		if err != nil && errors.IsNotFound(err) {
			// Job not found, create it
			logger.Info("Creating cluster bootstrap Job")
			job := r.bootstrapJobForRedisCluster(cluster)
			if err := controllerutil.SetControllerReference(cluster, job, r.Scheme); err != nil {
				logger.Error(err, "Failed to set owner reference on bootstrap Job")
				return ctrl.Result{}, err
			}
			if err := r.Create(ctx, job); err != nil {
				logger.Error(err, "Failed to create bootstrap Job")
				return ctrl.Result{}, err
			}
			return ctrl.Result{Requeue: true}, nil // Requeue to check Job status

		} else if err != nil {
			logger.Error(err, "Failed to get bootstrap Job")
			return ctrl.Result{}, err
		}

		// 9. Job already exists, check its status
		if bootstrapJob.Status.Succeeded > 0 {
			// Job succeeded! Update our CRD status
			logger.Info("Bootstrap Job succeeded. Updating cluster status.")
			cluster.Status.Initialized = true
			if err := r.Status().Update(ctx, cluster); err != nil {
				logger.Error(err, "Failed to update RedisCluster status")
				return ctrl.Result{}, err
			}
			logger.Info("Successfully reconciled and bootstrapped cluster")
			return ctrl.Result{}, nil
		}

		if bootstrapJob.Status.Failed > 0 {
			logger.Error(fmt.Errorf("bootstrap job %s failed", jobName), "Cluster initialization failed")
			return ctrl.Result{}, fmt.Errorf("bootstrap job %s failed", jobName)
		}

		if !cluster.Status.Initialized {
			logger.Info("Bootstrap job is still running...")
			return ctrl.Result{Requeue: true}, nil
		}
	}

	// --- AUTOSCALER ENTRY POINT ---
	// 10. If cluster IS initialized AND autoscaling is enabled, run the autoscaler
	if cluster.Status.Initialized && cluster.Spec.AutoScaleEnabled {
		logger.Info("Cluster is initialized, checking autoscaler")

		// Only check cooldown if we're NOT already in a scaling operation
		if !cluster.Status.IsResharding && !cluster.Status.IsDraining {
			// Check if we're in cooldown period (1 minute)
			if cluster.Status.LastScaleTime != nil {
				cooldownPeriod := time.Minute
				timeSinceLastScale := time.Since(cluster.Status.LastScaleTime.Time)
				if timeSinceLastScale < cooldownPeriod {
					logger.Info("In cooldown period, skipping autoscaling",
						"timeSinceLastScale", timeSinceLastScale.String(),
						"cooldownPeriod", cooldownPeriod.String())
					return ctrl.Result{RequeueAfter: cooldownPeriod - timeSinceLastScale}, nil
				}
			}
		}

		// This function is in autoscaler.go
		result, err := r.handleAutoScaling(ctx, cluster)
		if err != nil {
			logger.Error(err, "Failed to handle autoscaling")
			return ctrl.Result{}, err
		}

		// Update LastScaleTime ONLY when we initiate a NEW scale operation
		// (detected by the 1-second requeue AND not already scaling)
		if result.RequeueAfter == 1*time.Second && !cluster.Status.IsResharding && !cluster.Status.IsDraining {
			logger.Info("Scale operation initiated, setting cooldown timer.")
			now := metav1.Now()
			cluster.Status.LastScaleTime = &now
			if err := r.Status().Update(ctx, cluster); err != nil {
				logger.Error(err, "Failed to update LastScaleTime")
				// Don't block the requeue, just log the error
			}
		}

		return result, nil
	}

	logger.Info("Successfully reconciled. Autoscaling disabled.")
	// If not scaling, just requeue after 15 seconds to check again.
	return ctrl.Result{RequeueAfter: 15 * time.Second}, nil
}

// reconcileService handles creation and updates for the Service
func (r *RedisClusterReconciler) reconcileService(ctx context.Context, cluster *appv1.RedisCluster, desired *corev1.Service) error {
	if err := controllerutil.SetControllerReference(cluster, desired, r.Scheme); err != nil {
		return err
	}
	return r.reconcileResource(ctx, desired)
}

// reconcileStatefulSet handles creation and updates for the StatefulSet
func (r *RedisClusterReconciler) reconcileStatefulSet(ctx context.Context, cluster *appv1.RedisCluster, desired *appsv1.StatefulSet) error {
	if err := controllerutil.SetControllerReference(cluster, desired, r.Scheme); err != nil {
		return err
	}
	return r.reconcileResource(ctx, desired)
}

// reconcileServiceMonitor handles creation and updates for the ServiceMonitor
func (r *RedisClusterReconciler) reconcileServiceMonitor(ctx context.Context, cluster *appv1.RedisCluster, desired *monitoringv1.ServiceMonitor) error {
	if err := controllerutil.SetControllerReference(cluster, desired, r.Scheme); err != nil {
		return err
	}
	return r.reconcileResource(ctx, desired)
}

// reconcileConfigMap handles creation and updates for the ConfigMap
func (r *RedisClusterReconciler) reconcileConfigMap(ctx context.Context, cluster *appv1.RedisCluster, desired *corev1.ConfigMap) error {
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
			log.FromContext(ctx).Info("Creating new resource", "Kind", obj.GetObjectKind().GroupVersionKind().Kind, "Name", key.Name)
			return r.Create(ctx, obj)
		}
		return err
	}

	log.FromContext(ctx).Info("Updating existing resource", "Kind", obj.GetObjectKind().GroupVersionKind().Kind, "Name", key.Name)
	obj.SetResourceVersion(current.GetResourceVersion())
	return r.Update(ctx, obj)
}

// configMapForRedisCluster defines the desired ConfigMap for redis.conf
func (r *RedisClusterReconciler) configMapForRedisCluster(cluster *appv1.RedisCluster) *corev1.ConfigMap {
	labels := getLabels(cluster)
	config := `
port 6379
cluster-enabled yes
cluster-config-file /data/nodes.conf
cluster-node-timeout 5000
appendonly yes
bind 0.0.0.0
`
	return &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      cluster.Name + "-config",
			Namespace: cluster.Namespace,
			Labels:    labels,
		},
		Data: map[string]string{
			"redis.conf": config,
		},
	}
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
			ClusterIP: "None",
			Selector:  labels,
			Ports: []corev1.ServicePort{
				{Name: "redis", Port: 6379},
				{Name: "metrics", Port: 9121},
			},
		},
	}
}

// statefulSetForRedisCluster defines the desired StatefulSet
func (r *RedisClusterReconciler) statefulSetForRedisCluster(cluster *appv1.RedisCluster) *appsv1.StatefulSet {
	labels := getLabels(cluster)
	replicas := cluster.Spec.Masters * (1 + cluster.Spec.ReplicasPerMaster)

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
					Volumes: []corev1.Volume{
						{
							Name: "config",
							VolumeSource: corev1.VolumeSource{
								ConfigMap: &corev1.ConfigMapVolumeSource{
									LocalObjectReference: corev1.LocalObjectReference{
										Name: cluster.Name + "-config",
									},
								},
							},
						},
					},
					Containers: []corev1.Container{
						{
							Name:    "redis",
							Image:   fmt.Sprintf("redis:%s", cluster.Spec.RedisVersion),
							Command: []string{"redis-server", "/conf/redis.conf"},
							Ports: []corev1.ContainerPort{
								{ContainerPort: 6379, Name: "redis"},
							},
							VolumeMounts: []corev1.VolumeMount{
								{Name: "config", MountPath: "/conf"},
								{Name: "data", MountPath: "/data"},
							},
						},
						{
							Name:  "redis-exporter",
							Image: "bitnamilegacy/redis-exporter:1.59.0",
							Args:  []string{"--redis.addr=redis://localhost:6379"},
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
			VolumeClaimTemplates: []corev1.PersistentVolumeClaim{
				{
					ObjectMeta: metav1.ObjectMeta{Name: "data"},
					Spec: corev1.PersistentVolumeClaimSpec{
						AccessModes: []corev1.PersistentVolumeAccessMode{"ReadWriteOnce"},
						Resources: corev1.VolumeResourceRequirements{
							Requests: corev1.ResourceList{
								corev1.ResourceStorage: resource.MustParse("1Gi"),
							},
						},
					},
				},
			},
		},
	}
}

// bootstrapJobForRedisCluster defines the desired Job to initialize the cluster
func (r *RedisClusterReconciler) bootstrapJobForRedisCluster(cluster *appv1.RedisCluster) *batchv1.Job {
	serviceName := cluster.Name + "-headless"
	namespace := cluster.Namespace
	totalReplicas := cluster.Spec.Masters * (1 + cluster.Spec.ReplicasPerMaster)
	replicasFlag := cluster.Spec.ReplicasPerMaster

	var hosts []string
	for i := int32(0); i < totalReplicas; i++ {
		hosts = append(hosts, fmt.Sprintf("%s-%d.%s.%s.svc.cluster.local:6379", cluster.Name, i, serviceName, namespace))
	}
	hostString := strings.Join(hosts, " ")
	cliCmd := fmt.Sprintf("redis-cli --cluster create %s --cluster-replicas %d --cluster-yes", hostString, replicasFlag)

	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      cluster.Name + "-bootstrap",
			Namespace: cluster.Namespace,
			Labels:    getLabels(cluster),
		},
		Spec: batchv1.JobSpec{
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					RestartPolicy: corev1.RestartPolicyOnFailure,
					Containers: []corev1.Container{
						{
							Name:    "bootstrap",
							Image:   fmt.Sprintf("redis:%s", cluster.Spec.RedisVersion),
							Command: []string{"sh", "-c"},
							Args:    []string{cliCmd},
						},
					},
				},
			},
			BackoffLimit: new(int32),
		},
	}
}

// serviceMonitorForRedisCluster defines the desired ServiceMonitor
func (r *RedisClusterReconciler) serviceMonitorForRedisCluster(cluster *appv1.RedisCluster, svc *corev1.Service) *monitoringv1.ServiceMonitor {
	return &monitoringv1.ServiceMonitor{
		ObjectMeta: metav1.ObjectMeta{
			Name:      cluster.Name,
			Namespace: cluster.Namespace,
			Labels:    map[string]string{"release": "prometheus"},
		},
		Spec: monitoringv1.ServiceMonitorSpec{
			Selector: metav1.LabelSelector{
				MatchLabels: svc.Labels,
			},
			Endpoints: []monitoringv1.Endpoint{
				{
					Port:     "metrics",
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
		Owns(&appsv1.StatefulSet{}).
		Owns(&corev1.Service{}).
		Owns(&corev1.ConfigMap{}).
		Owns(&batchv1.Job{}).
		Owns(&monitoringv1.ServiceMonitor{}).
		Named("rediscluster").
		Complete(r)
}
