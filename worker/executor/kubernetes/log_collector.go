package kubernetes

import (
	"context"
	"fmt"
	"io"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/datazip-inc/olake-helm/worker/constants"
	"github.com/datazip-inc/olake-helm/worker/utils"
	"github.com/datazip-inc/olake-helm/worker/utils/logger"
	"github.com/spf13/viper"
)

func NewPodLogCollector(ctx context.Context, k *KubernetesExecutor, podName, podUID, workDir string) (*utils.ConnectorLogCollector, error) {
	return utils.NewConnectorLogCollector(
		ctx,
		workDir,
		func(ctx context.Context, lastLogTimestamp time.Time, follow bool) (io.Reader, error) {
			if err := k.checkPodUID(ctx, podName, podUID); err != nil {
				return nil, err
			}
			return openPodLogStream(ctx, k, podName, "connector", lastLogTimestamp, follow, false)
		},
		func(ctx context.Context) bool {
			pod, err := k.client.CoreV1().Pods(k.namespace).Get(ctx, podName, metav1.GetOptions{})
			if err != nil {
				return true
			}
			if podUID != "" && string(pod.UID) != podUID {
				return false
			}
			// check if pod is still running
			return pod.Status.Phase != corev1.PodSucceeded && pod.Status.Phase != corev1.PodFailed
		},
	)
}

func (k *KubernetesExecutor) checkPodUID(ctx context.Context, podName, podUID string) error {
	if podUID == "" {
		return nil
	}
	pod, err := k.client.CoreV1().Pods(k.namespace).Get(ctx, podName, metav1.GetOptions{})
	if err != nil {
		return err
	}
	if string(pod.UID) != podUID {
		return fmt.Errorf("execution outcome unknown: Kubernetes log resource UID changed")
	}
	return nil
}

func RecoverWorkerLogs(ctx context.Context, k *KubernetesExecutor) error {
	podName := workerPodName()

	logger.Infof("recovering worker logs from previous container for pod %s", podName)
	return utils.RecoverWorkerLogs(ctx, func(ctx context.Context) (io.Reader, error) {
		return openPodLogStream(ctx, k, podName, constants.WorkerContainerName, time.Time{}, false, true)
	})
}

func openPodLogStream(ctx context.Context, k *KubernetesExecutor, podName, container string, since time.Time, follow, previous bool) (io.Reader, error) {
	opts := &corev1.PodLogOptions{
		Container:  container,
		Follow:     follow,
		Previous:   previous,
		Timestamps: true,
	}
	if !since.IsZero() {
		opts.SinceTime = &metav1.Time{Time: since}
	}

	logs := k.client.CoreV1().Pods(k.namespace).GetLogs(podName, opts)
	stream, err := logs.Stream(ctx)
	if err != nil && previous {
		msg := strings.ToLower(err.Error())
		if strings.Contains(msg, "previous terminated container") || strings.Contains(msg, "a previous container log") {
			return nil, nil
		}
	}
	return stream, err
}

func workerPodName() string {
	return viper.GetString(constants.EnvPodName)
}
