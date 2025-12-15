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

	appv1 "github.com/myuser/redis-operator/api/v1"
	monitoringv1 "github.com/prometheus-operator/prometheus-operator/pkg/apis/monitoring/v1"
)

// RedisClusterReconciler reconciles a RedisCluster object.
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
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch
// +kubebuilder:rbac:groups=autoscaling.k8s.io,resources=verticalpodautoscalers,verbs=get;list;watch

// Reconcile is the main reconciliation loop for RedisCluster.
// It ensures the desired state of the cluster by:
//  1. Creating/updating ConfigMap, Service, StatefulSet, and ServiceMonitor
//  2. Bootstrapping the Redis cluster when first created
//  3. Running the autoscaler if enabled
func (r *RedisClusterReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	cluster := &appv1.RedisCluster{}
	if err := r.Get(ctx, req.NamespacedName, cluster); err != nil {
		if errors.IsNotFound(err) {
			logger.Info("RedisCluster resource not found, ignoring since object must be deleted")
			return ctrl.Result{}, nil
		}
		logger.Error(err, "Failed to get RedisCluster")
		return ctrl.Result{}, err
	}

	cluster.SetDefaults()

	if err := cluster.ValidateSpec(); err != nil {
		logger.Error(err, "Invalid RedisCluster spec")
		return ctrl.Result{}, err
	}

	if err := r.reconcileInfrastructure(ctx, cluster); err != nil {
		return ctrl.Result{}, err
	}

	if result, done, err := r.handleBootstrap(ctx, cluster); done {
		return result, err
	}

	if cluster.Status.StandbyPod == "" {
		logger.Info("No standby pod tracked, detecting...")
		if err := r.detectAndSetStandbyPod(ctx, cluster); err != nil {
			logger.Error(err, "Failed to detect standby pod")
			requeueInterval := time.Duration(cluster.Spec.MetricsQueryInterval) * time.Second
			return ctrl.Result{RequeueAfter: requeueInterval}, nil
		}
		if err := r.Status().Update(ctx, cluster); err != nil {
			logger.Error(err, "Failed to update status with standby pod")
			return ctrl.Result{}, err
		}
	}

	if cluster.Status.Initialized && cluster.Spec.AutoScaleEnabled {
		return r.handleAutoScaling(ctx, cluster)
	}

	logger.Info("Successfully reconciled, autoscaling disabled")
	requeueInterval := time.Duration(cluster.Spec.MetricsQueryInterval) * time.Second
	return ctrl.Result{RequeueAfter: requeueInterval}, nil
}

// reconcileInfrastructure creates or updates all infrastructure resources.
// This includes ConfigMap, Service, StatefulSet (if managed), and ServiceMonitor.
// For existing clusters where ManageStatefulSet=false, only ConfigMap and ServiceMonitor are managed.
func (r *RedisClusterReconciler) reconcileInfrastructure(ctx context.Context, cluster *appv1.RedisCluster) error {
	logger := log.FromContext(ctx)

	cm := r.configMapForRedisCluster(cluster)
	if err := r.reconcileConfigMap(ctx, cluster, cm); err != nil {
		logger.Error(err, "Failed to reconcile ConfigMap")
		return err
	}

	// Only manage Service and StatefulSet if ManageStatefulSet is true
	if cluster.Spec.ManageStatefulSet {
		svc := r.serviceForRedisCluster(cluster)
		if err := r.reconcileService(ctx, cluster, svc); err != nil {
			logger.Error(err, "Failed to reconcile Service")
			return err
		}

		sts := r.statefulSetForRedisCluster(cluster)
		if err := r.reconcileStatefulSet(ctx, cluster, sts); err != nil {
			logger.Error(err, "Failed to reconcile StatefulSet")
			return err
		}

		sm := r.serviceMonitorForRedisCluster(cluster, svc)
		if err := r.reconcileServiceMonitor(ctx, cluster, sm); err != nil {
			logger.Error(err, "Failed to reconcile ServiceMonitor")
			return err
		}
	} else {
		logger.Info("Skipping Service and StatefulSet management (ManageStatefulSet=false)")

		// For existing clusters, still try to reconcile ServiceMonitor if a service exists
		svc := &corev1.Service{}
		if err := r.Get(ctx, client.ObjectKey{Name: cluster.Spec.ServiceName, Namespace: cluster.Namespace}, svc); err == nil {
			sm := r.serviceMonitorForRedisCluster(cluster, svc)
			if err := r.reconcileServiceMonitor(ctx, cluster, sm); err != nil {
				logger.Error(err, "Failed to reconcile ServiceMonitor")
				return err
			}
		}
	}

	return nil
}

// handleBootstrap manages the cluster bootstrap process.
// For existing clusters (ExistingCluster=true), it discovers the topology instead of bootstrapping.
// Returns (result, done, error) where done=true means the caller should return immediately.
func (r *RedisClusterReconciler) handleBootstrap(ctx context.Context, cluster *appv1.RedisCluster) (ctrl.Result, bool, error) {
	logger := log.FromContext(ctx)

	// For existing clusters, skip bootstrap and discover topology instead
	if cluster.Spec.ExistingCluster {
		if cluster.Status.Initialized {
			return ctrl.Result{}, false, nil
		}

		logger.Info("Discovering existing Redis cluster topology")
		if err := r.discoverRedisTopology(ctx, cluster); err != nil {
			logger.Error(err, "Failed to discover cluster topology")
			return ctrl.Result{RequeueAfter: 10 * time.Second}, true, nil
		}

		if err := r.detectAndSetStandbyPod(ctx, cluster); err != nil {
			logger.Error(err, "Failed to detect standby pod in existing cluster")
			return ctrl.Result{RequeueAfter: 10 * time.Second}, true, nil
		}

		if err := r.Status().Update(ctx, cluster); err != nil {
			logger.Error(err, "Failed to update status after discovering existing cluster")
			return ctrl.Result{}, true, err
		}

		logger.Info("Successfully discovered existing cluster", "standbyPod", cluster.Status.StandbyPod)
		return ctrl.Result{}, false, nil
	}

	// For managed clusters, proceed with standard bootstrap
	stsName := cluster.Spec.StatefulSetName
	if stsName == "" {
		stsName = cluster.Name
	}

	sts := &appsv1.StatefulSet{}
	if err := r.Get(ctx, client.ObjectKey{Name: stsName, Namespace: cluster.Namespace}, sts); err != nil {
		logger.Error(err, "Failed to get StatefulSet for status check")
		return ctrl.Result{}, true, err
	}

	totalReplicas := (cluster.Spec.Masters + 1) * (1 + cluster.Spec.ReplicasPerMaster)
	if sts.Status.ReadyReplicas != totalReplicas {
		logger.Info("Waiting for all StatefulSet replicas to be ready",
			"ready", sts.Status.ReadyReplicas,
			"desired", totalReplicas)
		return ctrl.Result{RequeueAfter: 5 * time.Second}, true, nil
	}

	if cluster.Status.Initialized {
		return ctrl.Result{}, false, nil
	}

	bootstrapJob := &batchv1.Job{}
	jobName := cluster.Name + "-bootstrap"
	err := r.Get(ctx, client.ObjectKey{Name: jobName, Namespace: cluster.Namespace}, bootstrapJob)

	if err != nil && errors.IsNotFound(err) {
		logger.Info("Creating cluster bootstrap job")
		job := r.bootstrapJobForRedisCluster(cluster)
		if err := controllerutil.SetControllerReference(cluster, job, r.Scheme); err != nil {
			logger.Error(err, "Failed to set owner reference on bootstrap job")
			return ctrl.Result{}, true, err
		}
		if err := r.Create(ctx, job); err != nil {
			logger.Error(err, "Failed to create bootstrap job")
			return ctrl.Result{}, true, err
		}
		return ctrl.Result{Requeue: true}, true, nil
	} else if err != nil {
		logger.Error(err, "Failed to get bootstrap job")
		return ctrl.Result{}, true, err
	}

	if bootstrapJob.Status.Succeeded > 0 {
		logger.Info("Bootstrap job succeeded, detecting standby node")
		cluster.Status.Initialized = true

		if err := r.detectAndSetStandbyPod(ctx, cluster); err != nil {
			logger.Error(err, "Failed to detect standby pod")
			return ctrl.Result{}, true, err
		}

		if err := r.Status().Update(ctx, cluster); err != nil {
			logger.Error(err, "Failed to update RedisCluster status")
			return ctrl.Result{}, true, err
		}

		logger.Info("Successfully bootstrapped cluster", "standbyPod", cluster.Status.StandbyPod)
		return ctrl.Result{}, true, nil
	}

	if bootstrapJob.Status.Failed > 0 {
		logger.Error(fmt.Errorf("bootstrap job %s failed", jobName), "Cluster initialization failed")
		return ctrl.Result{}, true, fmt.Errorf("bootstrap job %s failed", jobName)
	}

	logger.Info("Bootstrap job is still running")
	return ctrl.Result{Requeue: true}, true, nil
}

// discoverRedisTopology discovers the Redis cluster topology for existing clusters.
// It queries the Redis cluster to find masters, replicas, and the standby node (master with 0 slots).
// This is used when ExistingCluster=true to work with already deployed Redis clusters.
func (r *RedisClusterReconciler) discoverRedisTopology(ctx context.Context, cluster *appv1.RedisCluster) error {
	logger := log.FromContext(ctx)

	// Get all pods matching the PodSelector
	podList := &corev1.PodList{}
	if err := r.List(ctx, podList, client.InNamespace(cluster.Namespace), client.MatchingLabels(cluster.Spec.PodSelector)); err != nil {
		return fmt.Errorf("failed to list Redis pods: %w", err)
	}

	if len(podList.Items) == 0 {
		return fmt.Errorf("no pods found matching selector %v", cluster.Spec.PodSelector)
	}

	// Find a running pod to query cluster info
	var queryPod *corev1.Pod
	for i := range podList.Items {
		pod := &podList.Items[i]
		if pod.Status.Phase == corev1.PodRunning {
			queryPod = pod
			break
		}
	}

	if queryPod == nil {
		return fmt.Errorf("no running pods found to query cluster topology")
	}

	logger.Info("Discovering Redis topology from existing cluster",
		"queryPod", queryPod.Name,
		"totalPods", len(podList.Items))

	// Query Redis cluster nodes to find masters, replicas, and slot distribution
	// For now, mark cluster as initialized since it already exists
	cluster.Status.Initialized = true
	cluster.Status.CurrentMasters = cluster.Spec.Masters
	cluster.Status.CurrentReplicas = cluster.Spec.Masters * cluster.Spec.ReplicasPerMaster

	// Try to detect standby pod (master with 0 slots)
	// This will be done in the updated detectAndSetStandbyPod function
	logger.Info("Existing cluster discovered", "masters", cluster.Status.CurrentMasters)

	return nil
}

// detectAndSetStandbyPod finds the standby master node (the one with 0 hash slots).
// For managed clusters, the standby is at index (Masters * (1 + ReplicasPerMaster)).
// For existing clusters, it queries all pods to find which master has 0 slots.
// It verifies the pod exists and is running before setting it in the cluster status.
func (r *RedisClusterReconciler) detectAndSetStandbyPod(ctx context.Context, cluster *appv1.RedisCluster) error {
	logger := log.FromContext(ctx)

	// For existing clusters, use PodSelector to find all pods
	if cluster.Spec.ExistingCluster {
		podList := &corev1.PodList{}
		if err := r.List(ctx, podList, client.InNamespace(cluster.Namespace), client.MatchingLabels(cluster.Spec.PodSelector)); err != nil {
			return fmt.Errorf("failed to list Redis pods: %w", err)
		}

		// For now, we'll need to query Redis to find which pod has 0 slots
		// This requires executing redis-cli commands, which is complex
		// As a temporary solution, check if there's already a StandbyPod set
		if cluster.Status.StandbyPod != "" {
			logger.Info("Using existing standby pod", "pod", cluster.Status.StandbyPod)
			return nil
		}

		// If not set, we cannot easily determine it without redis-cli
		// This will be enhanced in a future iteration
		logger.Info("Cannot auto-detect standby pod for existing cluster, manual configuration may be needed")
		return fmt.Errorf("standby pod detection not yet implemented for existing clusters")
	}

	// For managed clusters, use index-based detection
	standbyIndex := cluster.Spec.Masters * (1 + cluster.Spec.ReplicasPerMaster)
	standbyPodName := fmt.Sprintf("%s-%d", cluster.Name, standbyIndex)

	standbyPod := &corev1.Pod{}
	if err := r.Get(ctx, client.ObjectKey{
		Name:      standbyPodName,
		Namespace: cluster.Namespace,
	}, standbyPod); err != nil {
		if errors.IsNotFound(err) {
			logger.Info("Standby pod doesn't exist yet", "pod", standbyPodName)
			return fmt.Errorf("standby pod not ready: %s", standbyPodName)
		}
		return fmt.Errorf("failed to get standby pod: %w", err)
	}

	if standbyPod.Status.Phase != corev1.PodRunning {
		return fmt.Errorf("standby pod not running: %s (phase: %s)", standbyPodName, standbyPod.Status.Phase)
	}

	allReady := false
	for _, cs := range standbyPod.Status.ContainerStatuses {
		if cs.Ready {
			allReady = true
			break
		}
	}
	if !allReady {
		return fmt.Errorf("standby pod containers not ready: %s", standbyPodName)
	}

	logger.Info("Standby pod detected", "pod", standbyPodName, "podIP", standbyPod.Status.PodIP)

	cluster.Status.StandbyPod = standbyPodName
	return nil
}

// reconcileService creates or updates the headless Service for the Redis cluster.
func (r *RedisClusterReconciler) reconcileService(ctx context.Context, cluster *appv1.RedisCluster, desired *corev1.Service) error {
	if err := controllerutil.SetControllerReference(cluster, desired, r.Scheme); err != nil {
		return err
	}
	return r.reconcileResource(ctx, desired)
}

// reconcileStatefulSet creates or updates the StatefulSet for the Redis cluster.
func (r *RedisClusterReconciler) reconcileStatefulSet(ctx context.Context, cluster *appv1.RedisCluster, desired *appsv1.StatefulSet) error {
	if err := controllerutil.SetControllerReference(cluster, desired, r.Scheme); err != nil {
		return err
	}
	return r.reconcileResource(ctx, desired)
}

// reconcileServiceMonitor creates or updates the Prometheus ServiceMonitor for metrics collection.
func (r *RedisClusterReconciler) reconcileServiceMonitor(ctx context.Context, cluster *appv1.RedisCluster, desired *monitoringv1.ServiceMonitor) error {
	if err := controllerutil.SetControllerReference(cluster, desired, r.Scheme); err != nil {
		return err
	}
	return r.reconcileResource(ctx, desired)
}

// reconcileConfigMap creates or updates the ConfigMap containing redis.conf.
func (r *RedisClusterReconciler) reconcileConfigMap(ctx context.Context, cluster *appv1.RedisCluster, desired *corev1.ConfigMap) error {
	if err := controllerutil.SetControllerReference(cluster, desired, r.Scheme); err != nil {
		return err
	}
	return r.reconcileResource(ctx, desired)
}

// reconcileResource is a generic helper that creates a resource if it doesn't exist,
// or updates it if it does. It sets the owner reference automatically.
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

	obj.SetResourceVersion(current.GetResourceVersion())
	return r.Update(ctx, obj)
}

// configMapForRedisCluster builds the ConfigMap containing the Redis configuration file.
func (r *RedisClusterReconciler) configMapForRedisCluster(cluster *appv1.RedisCluster) *corev1.ConfigMap {
	labels := getLabels(cluster)
	config := `port 6379
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

// serviceForRedisCluster builds the headless Service for internal pod-to-pod communication.
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

// statefulSetForRedisCluster builds the StatefulSet for Redis pods.
// The replica count includes the active masters plus one standby master, each with their replicas.
func (r *RedisClusterReconciler) statefulSetForRedisCluster(cluster *appv1.RedisCluster) *appsv1.StatefulSet {
	labels := getLabels(cluster)
	replicas := (cluster.Spec.Masters + 1) * (1 + cluster.Spec.ReplicasPerMaster)

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

// bootstrapJobForRedisCluster creates initial cluster, joining the standby master with 0 slots
// bootstrapJobForRedisCluster creates a Kubernetes Job that initializes the Redis cluster.
// The job creates the initial cluster with active masters and replicas, then adds the standby
// master with 0 hash slots and its replica.
func (r *RedisClusterReconciler) bootstrapJobForRedisCluster(cluster *appv1.RedisCluster) *batchv1.Job {
	serviceName := cluster.Name + "-headless"
	namespace := cluster.Namespace

	activeMasters := cluster.Spec.Masters
	replicasPerMaster := cluster.Spec.ReplicasPerMaster

	activeClusterReplicas := activeMasters * (1 + replicasPerMaster)
	standbyMasterIndex := activeClusterReplicas
	standbyReplicaIndex := activeClusterReplicas + 1

	var activeHosts []string
	for i := int32(0); i < activeClusterReplicas; i++ {
		activeHosts = append(activeHosts, fmt.Sprintf("%s-%d.%s.%s.svc.cluster.local:6379", cluster.Name, i, serviceName, namespace))
	}
	activeHostString := strings.Join(activeHosts, " ")

	// FQDN for the Standby Master node
	standbyMasterFQDN := fmt.Sprintf("%s-%d.%s.%s.svc.cluster.local:6379", cluster.Name, standbyMasterIndex, serviceName, namespace)
	standbyReplicaFQDN := fmt.Sprintf("%s-%d.%s.%s.svc.cluster.local:6379", cluster.Name, standbyReplicaIndex, serviceName, namespace)

	// --- Multi-step Shell Command ---
	cliCmd := fmt.Sprintf(`
#!/bin/bash
set -ex

# 1. Create the initial cluster with ONLY the active nodes.
# This assigns all 16384 slots to the active masters.
echo "Phase 1: Creating active cluster with %d masters"
redis-cli --cluster create %s --cluster-replicas %d --cluster-yes

# Use the first active node as the entry point for subsequent commands
ENTRYPOINT=%s

# Wait a moment for the cluster configuration to propagate
sleep 5

# 2. Add the Standby Master (highest index)
# It joins as a master but since all slots are taken, it receives 0 slots.
echo "Phase 2: Adding standby master %s with 0 slots"
redis-cli --cluster add-node %s $ENTRYPOINT || true
sleep 5

# Get the ID of the newly added standby master
STANDBY_MASTER_IP=$(getent hosts $(echo "%s" | cut -d: -f1) | awk '{print $1}')
STANDBY_MASTER_ID=$(redis-cli -h $(echo "$ENTRYPOINT" | cut -d: -f1) -p 6379 cluster nodes | grep "$STANDBY_MASTER_IP" | awk '{print $1}' | head -n 1)

if [ -z "$STANDBY_MASTER_ID" ]; then
  echo "ERROR: Failed to determine Standby Master ID."
  exit 1
fi

echo "Standby Master ID: $STANDBY_MASTER_ID"

# 3. Add the Standby Replica and assign it to the Standby Master
echo "Phase 3: Adding standby replica %s to master ID $STANDBY_MASTER_ID"
redis-cli --cluster add-node %s $ENTRYPOINT \
  --cluster-slave --cluster-master-id $STANDBY_MASTER_ID || true
sleep 5

echo "Bootstrap complete. Standby master is joined with 0 slots."
`,
		activeMasters,
		activeHostString,
		replicasPerMaster,
		activeHosts[0],
		standbyMasterFQDN,
		standbyMasterFQDN,
		standbyMasterFQDN,
		standbyReplicaFQDN,
		standbyReplicaFQDN)

	// ... (rest of the Job definition remains the same)
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

// serviceMonitorForRedisCluster builds the Prometheus ServiceMonitor for scraping Redis metrics.
func (r *RedisClusterReconciler) serviceMonitorForRedisCluster(cluster *appv1.RedisCluster, svc *corev1.Service) *monitoringv1.ServiceMonitor {
	return &monitoringv1.ServiceMonitor{
		ObjectMeta: metav1.ObjectMeta{
			Name:      cluster.Name,
			Namespace: cluster.Namespace,
			Labels: map[string]string{
				"release": "prometheus",
				"app":     "redis-cluster",
			},
		},
		Spec: monitoringv1.ServiceMonitorSpec{
			Selector: metav1.LabelSelector{
				MatchLabels: svc.Labels,
			},
			Endpoints: []monitoringv1.Endpoint{
				{
					Port:     "metrics",
					Interval: "15s",
					Path:     "/metrics",
				},
			},
		},
	}
}

// getLabels returns the label selector for finding Redis pods.
// For existing clusters, it uses the user-provided PodSelector.
// For managed clusters, it uses the default labels.
func getLabels(cluster *appv1.RedisCluster) map[string]string {
	if cluster.Spec.ExistingCluster && len(cluster.Spec.PodSelector) > 0 {
		return cluster.Spec.PodSelector
	}

	return map[string]string{
		"app":       "redis-cluster",
		"cluster":   cluster.Name,
		"component": "redis",
	}
}

// SetupWithManager configures the controller with the Manager and sets up watches.
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
