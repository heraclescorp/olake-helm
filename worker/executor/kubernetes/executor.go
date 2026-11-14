package kubernetes

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/datazip-inc/olake-helm/worker/constants"
	"github.com/datazip-inc/olake-helm/worker/storage"
	"github.com/datazip-inc/olake-helm/worker/types"
	"github.com/datazip-inc/olake-helm/worker/utils"
	"github.com/datazip-inc/olake-helm/worker/utils/logger"
	"github.com/spf13/viper"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

const (
	DefaultQPS   = 50  // DefaultQPS defines the maximum queries per second allowed to the Kubernetes API server
	DefaultBurst = 100 // DefaultBurst defines the maximum number of requests allowed in a burst to the Kubernetes API server
)

type KubernetesExecutor struct {
	client        kubernetes.Interface
	namespace     string
	config        *KubernetesConfig
	configWatcher *ConfigMapWatcher
}

type KubernetesConfig struct {
	Namespace           string
	PVCName             string
	S3CredentialsSecret string
	ServiceAccount      string
	JobServiceAccount   string
	SecretKey           string
	BasePath            string
	WorkerIdentity      string
	SecurityContext     *corev1.PodSecurityContext
	JobPodAnnotations   map[string]string
	JobPodLabels        map[string]string
}

func NewKubernetesExecutor(ctx context.Context) (*KubernetesExecutor, error) {
	// Use in-cluster configuration - this reads the service account token and CA cert
	// that Kubernetes automatically mounts into every pod at /var/run/secrets/kubernetes.io/serviceaccount/
	clusterConfig, err := rest.InClusterConfig()
	if err != nil {
		return nil, fmt.Errorf("failed to get in-cluster config: %s", err)
	}

	// Configure QPS and Burst limits to avoid client-side throttling
	clusterConfig.QPS = DefaultQPS
	clusterConfig.Burst = DefaultBurst

	// Create the Kubernetes clientset using the in-cluster config
	// This clientset provides access to all Kubernetes API operations (pods, services, etc.)
	clientset, err := kubernetes.NewForConfig(clusterConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create Kubernetes client: %s", err)
	}

	// Get config from environment
	namespace := viper.GetString(constants.EnvNamespace)
	pvcName := viper.GetString(constants.EnvStoragePVCName)
	s3CredentialsSecret := viper.GetString(constants.EnvS3CredentialsSecret)
	serviceAccount := viper.GetString(constants.EnvJobServiceAccountName)
	jobServiceAccount := viper.GetString(constants.EnvJobServiceAccountName)
	secretKey := viper.GetString(constants.EnvSecretKey)
	basePath := storage.ConfigDir()

	// Parse security context JSON if available
	var securityContext *corev1.PodSecurityContext
	securityContextJSON := viper.GetString(constants.EnvPodSecurityContext)
	if securityContextJSON != "" {
		securityContext = &corev1.PodSecurityContext{}
		if err := json.Unmarshal([]byte(securityContextJSON), securityContext); err != nil {
			logger.Errorf("failed to unmarshal job security context: %s. using default.", err)
			securityContext = nil // Reset to nil on error
		}
	}

	// Parse job pod annotations JSON if available
	var jobPodAnnotations map[string]string
	jobPodAnnotationsJSON := viper.GetString(constants.EnvJobPodAnnotations)
	if jobPodAnnotationsJSON != "" {
		if err := json.Unmarshal([]byte(jobPodAnnotationsJSON), &jobPodAnnotations); err != nil {
			logger.Errorf("failed to unmarshal job pod annotations: %s. using default.", err)
			jobPodAnnotations = nil
		}
	}

	var jobPodLabels map[string]string
	if raw := viper.GetString(constants.EnvJobPodLabels); raw != "" {
		if err := json.Unmarshal([]byte(raw), &jobPodLabels); err != nil {
			logger.Errorf("failed to unmarshal job pod labels: %s. using default.", err)
			jobPodLabels = nil
		}
	}

	// Set worker identity
	podName := viper.GetString(constants.EnvPodName)
	workerIdenttity := fmt.Sprintf("olake.io/olake-workers/%s", podName)

	watcher := NewConfigMapWatcher(ctx, clientset, namespace)
	if err := watcher.Start(); err != nil {
		logger.Errorf("failed to start config map watcher: %s", err)
	}

	return &KubernetesExecutor{
		client:        clientset,
		namespace:     namespace,
		configWatcher: watcher,
		config: &KubernetesConfig{
			Namespace:           namespace,
			PVCName:             pvcName,
			S3CredentialsSecret: s3CredentialsSecret,
			ServiceAccount:      serviceAccount,
			JobServiceAccount:   jobServiceAccount,
			SecretKey:           secretKey,
			BasePath:            basePath,
			WorkerIdentity:      workerIdenttity,
			SecurityContext:     securityContext,
			JobPodAnnotations:   jobPodAnnotations,
			JobPodLabels:        jobPodLabels,
		},
	}, nil
}

func (k *KubernetesExecutor) Execute(ctx context.Context, req *types.ExecutionRequest, workdir string) (string, error) {
	log := logger.Log(ctx)
	imageName := utils.GetDockerImageName(req.ConnectorType, req.Version)

	controlled := utils.IsAsyncCommand(req.Command) && req.Controlled
	if req.Command == types.Resync && !controlled {
		return "", fmt.Errorf("resync requires a supported controlled execution domain")
	}
	var pod *corev1.Pod
	if controlled {
		if req.RuntimeID == "" {
			return "", fmt.Errorf("execution outcome unknown: asynchronous Kubernetes execution requires admitted runtime identity")
		}
		var err error
		pod, err = k.findControlledPod(ctx, req)
		if err != nil {
			return "", err
		}
		if pod == nil && (!req.AllowLaunch || req.ResourceID != "") {
			return "", fmt.Errorf("execution outcome unknown: admitted Kubernetes resource is missing")
		}
		if pod != nil {
			if err := validatePodAttachment(pod); err != nil {
				return "", err
			}
		}
	}
	if pod == nil {
		// Provision the per-job index only for an actual launch, never an attach retry.
		indexVolume, err := k.ensureIndexVolume(ctx, req.JobID, req.Command, req.IndexRequired, req.HeartbeatFunc)
		if err != nil {
			log.Error("failed to prepare index volume", "jobID", req.JobID, "command", req.Command, "error", err)
			return "", err
		}
		podSpec, err := k.CreatePodSpec(req, workdir, imageName, indexVolume)
		if err != nil {
			return "", err
		}
		log.Info(fmt.Sprintf("Starting pod %s with image %s", podSpec.Name, imageName))
		if controlled {
			pod, err = k.createControlledPod(ctx, req, podSpec)
		} else {
			pod, err = k.createPod(ctx, podSpec)
		}
		if err != nil {
			log.Error("failed to create pod", "podName", podSpec.Name, "error", err)
			return "", err
		}
	}
	podUID := ""
	if controlled {
		podUID = string(pod.UID)
	}

	if !utils.IsAsyncCommand(req.Command) {
		defer func() {
			utils.ReleaseConnectorLogCollector(workdir)
			cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), time.Second*constants.ContainerCleanupTimeout)
			defer cancel()

			if err := k.cleanupPod(cleanupCtx, pod.Name); err != nil {
				log.Error("failed to cleanup pod", "podName", pod.Name, "command", req.Command, "workflowID", req.WorkflowID, "error", err)
			}
		}()
	}

	if storage.Mode() == constants.StorageModeS3 {
		// Stream connector logs to S3 while the pod runs; Release later stops it with a final catch-up and flush.
		if err := utils.AcquireConnectorLogCollector(ctx, workdir, func() (*utils.ConnectorLogCollector, error) {
			return NewPodLogCollector(ctx, k, pod.Name, podUID, workdir)
		}); err != nil {
			return "", fmt.Errorf("failed to start connector log collector: %s", err)
		}
	}

	if err := k.waitForPodCompletion(ctx, pod.Name, podUID, req.Timeout, req.HeartbeatFunc); err != nil {
		if req.Command != types.Sync { // SyncActivity explains sync failures itself
			log.Error("pod failed to complete", "podName", pod.Name, "error", err)
		}
		return "", err
	}

	if err := k.checkPodUID(ctx, pod.Name, podUID); err != nil {
		return "", err
	}
	logs, err := k.getPodLogs(ctx, pod.Name, nil)
	if err != nil {
		log.Error("failed to get pod logs", "podName", pod.Name, "error", err)
		return "", fmt.Errorf("failed to get pod logs: %s", err)
	}

	return logs, nil
}

func (k *KubernetesExecutor) Cleanup(ctx context.Context, req *types.ExecutionRequest) error {
	log := logger.Log(ctx)
	podName := k.podResourceName(req)
	log.Info("cleaning up pod", "podName", podName, "workflowID", req.WorkflowID)

	_, workDir := utils.GetExecutionDirAndSubDir(req)
	utils.ReleaseConnectorLogCollector(workDir)
	if utils.IsAsyncCommand(req.Command) && req.Controlled {
		if req.RuntimeID == "" {
			return fmt.Errorf("execution outcome unknown: Kubernetes cleanup requires admitted runtime identity")
		}
		return k.cleanupControlledPod(ctx, req)
	}

	if err := k.cleanupPod(ctx, podName); err != nil {
		log.Error("failed to cleanup pod", "podName", podName, "error", err)
		return fmt.Errorf("failed to cleanup pod: %s", err)
	}
	if utils.IsAsyncCommand(req.Command) {
		if err := wait.PollUntilContextTimeout(ctx, time.Second, time.Second*constants.ContainerCleanupTimeout, true, func(ctx context.Context) (bool, error) {
			_, err := k.client.CoreV1().Pods(k.namespace).Get(ctx, podName, metav1.GetOptions{})
			if apierrors.IsNotFound(err) {
				return true, nil
			}
			return false, err
		}); err != nil {
			return fmt.Errorf("pod termination is unconfirmed: %w", err)
		}
	}

	log.Info("pod cleanup completed", "podName", podName)
	return nil
}

func (k *KubernetesExecutor) Close() error {
	k.configWatcher.cancel()
	return nil
}
