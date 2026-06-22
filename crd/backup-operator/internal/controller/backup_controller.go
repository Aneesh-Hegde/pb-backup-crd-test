package controller

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"

	corev1alpha1 "github.com/Aneesh-Hegde/pb-backup-crd/api/v1alpha1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

type BackupReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=core.pointblank.com,resources=backups,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core.pointblank.com,resources=backups/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=core.pointblank.com,resources=backups/finalizers,verbs=update
// +kubebuilder:rbac:groups=batch,resources=cronjobs,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=services;secrets;configmaps,verbs=get;list;watch;create;update;patch;delete

func (r *BackupReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	var backup corev1alpha1.Backup
	if err := r.Get(ctx, req.NamespacedName, &backup); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	logger.Info("Reconciling Backup definition", "App", backup.Spec.TargetApp)

	// INITIALIZE STATUS
	if len(backup.Status.Conditions) == 0 {
		meta.SetStatusCondition(&backup.Status.Conditions, metav1.Condition{
			Type:    "Ready",
			Status:  metav1.ConditionFalse,
			Reason:  "Initializing",
			Message: "Starting reconciliation and provisioning",
		})
		if err := r.Status().Update(ctx, &backup); err != nil {
			logger.Error(err, "Failed to update initial status")
			return ctrl.Result{}, err
		}
		return ctrl.Result{Requeue: true}, nil
	}

	// DYNAMIC FALLBACKS
	mountPath := backup.Spec.MountPath
	if mountPath == "" {
		mountPath = "/data"
	}
	endpoint := backup.Spec.Endpoint
	if endpoint == "" {
		var garageSvc corev1.Service
		err := r.Get(ctx, types.NamespacedName{Name: "garage", Namespace: "garage"}, &garageSvc)
		if err == nil && len(garageSvc.Spec.Ports) > 0 {
			dynamicPort := garageSvc.Spec.Ports[0].Port
			endpoint = fmt.Sprintf("http://garage.garage.svc.cluster.local:%d", dynamicPort)
		} else {
			endpoint = "http://garage.garage.svc.cluster.local:3906"
		}
	}

	bucketName := backup.Spec.BucketName
	if bucketName == "" {
		bucketName = fmt.Sprintf("%s-backups", backup.Name)
	}

	secretName := backup.Spec.CredentialsSecret

	// ZERO-TOUCH S3 PROVISIONING
	storageProvisioned := meta.IsStatusConditionTrue(backup.Status.Conditions, "StorageProvisioned")

	if secretName == "" {
		secretName = fmt.Sprintf("%s-s3-credentials", backup.Name)

		if !storageProvisioned {
			var existingSecret corev1.Secret
			secretExists := r.Get(ctx, types.NamespacedName{
				Name: secretName, Namespace: backup.Namespace,
			}, &existingSecret) == nil

			if !secretExists {
				logger.Info("No credentialsSecret provided. Auto-provisioning Garage S3 Bucket and Keys...", "Bucket", bucketName)
				err := r.provisionGarageResources(ctx, backup.Namespace, secretName, bucketName)
				if err != nil {
					meta.SetStatusCondition(&backup.Status.Conditions, metav1.Condition{
						Type:    "StorageProvisioned",
						Status:  metav1.ConditionFalse,
						Reason:  "ProvisioningFailed",
						Message: fmt.Sprintf("Garage API Error: %v", err),
					})
					r.Status().Update(ctx, &backup)
					logger.Error(err, "Failed to auto-provision Garage resources")
					return ctrl.Result{}, err
				}
			}

			meta.SetStatusCondition(&backup.Status.Conditions, metav1.Condition{
				Type:    "StorageProvisioned",
				Status:  metav1.ConditionTrue,
				Reason:  "ProvisioningSucceeded",
				Message: "Garage S3 bucket and secrets successfully created",
			})
			r.Status().Update(ctx, &backup)
			logger.Info("Successfully generated S3 resources and Kubernetes Secret", "SecretName", secretName)
			return ctrl.Result{Requeue: true}, nil
		}
	}

	// DYNAMIC BLUEPRINT FETCHING
	blueprintName := fmt.Sprintf("backup-blueprint-%s", backup.Spec.DatabaseType)
	if backup.Spec.DatabaseType == "" {
		blueprintName = "backup-blueprint-default"
	}

	var blueprint corev1.ConfigMap
	if err := r.Get(ctx, types.NamespacedName{Name: blueprintName, Namespace: "garage"}, &blueprint); err != nil {
		logger.Info("Blueprint not ready yet, requeuing in 10s", "BlueprintName", blueprintName)
		meta.SetStatusCondition(&backup.Status.Conditions, metav1.Condition{
			Type:    "Ready",
			Status:  metav1.ConditionFalse,
			Reason:  "BlueprintMissing",
			Message: fmt.Sprintf("Missing engine blueprint ConfigMap '%s' in 'garage' namespace", blueprintName),
		})
		r.Status().Update(ctx, &backup)
		return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
	}

	appDumpLogic := blueprint.Data["backup.sh"]
	blueprintImage := blueprint.Data["image"]

	image := backup.Spec.Image
	if image == "" && blueprintImage != "" {
		image = blueprintImage
	} else if image == "" {
		image = "amazon/aws-cli:latest"
	}

	// ENVIRONMENT VARIABLES
	envVars := []corev1.EnvVar{
		{
			Name: "AWS_ACCESS_KEY_ID",
			ValueFrom: &corev1.EnvVarSource{
				SecretKeyRef: &corev1.SecretKeySelector{
					LocalObjectReference: corev1.LocalObjectReference{Name: secretName},
					Key:                  "AWS_ACCESS_KEY_ID",
				},
			},
		},
		{
			Name: "AWS_SECRET_ACCESS_KEY",
			ValueFrom: &corev1.EnvVarSource{
				SecretKeyRef: &corev1.SecretKeySelector{
					LocalObjectReference: corev1.LocalObjectReference{Name: secretName},
					Key:                  "AWS_SECRET_ACCESS_KEY",
				},
			},
		},
		{
			Name:  "AWS_DEFAULT_REGION",
			Value: "us-east-1",
		},
		{
			Name:  "MOUNT_PATH",
			Value: mountPath,
		},
	}

	if len(backup.Spec.DatabaseEnv) > 0 {
		envVars = append(envVars, backup.Spec.DatabaseEnv...)
	}

	// AWS UPLOAD LOGIC
	s3UploadLogic := fmt.Sprintf(`set -e
echo "Starting S3 upload pipeline..."
TIMESTAMP=$(date +%%Y%%m%%d-%%H%%M%%S)
FILENAME="%s-backup-$TIMESTAMP.tar.gz"

echo "Renaming raw dump to $FILENAME..."
mv /workspace/dump.archive /workspace/$FILENAME

echo "Uploading to S3 bucket: %s..."
aws s3 cp /workspace/$FILENAME s3://%s/$FILENAME --endpoint-url %s
`, backup.Spec.TargetApp, bucketName, bucketName, endpoint)

	// RETENTION LOGIC
	retentionLogic := ""
	if backup.Spec.RetentionDays > 0 {
		retentionLogic = fmt.Sprintf(`
echo "Executing Retention Policy for backups older than %d days..."
CUTOFF=$(date -d "-%d days" +%%s)

aws s3api list-objects --bucket %s --endpoint-url %s | \
  jq -r --argjson cutoff "$CUTOFF" '(.Contents // [])[] | select(.LastModified | fromdateiso8601 < $cutoff) | .Key' | \
  while read -r key; do
    if [ -n "$key" ] && [ "$key" != "null" ]; then
      echo "Deleting old backup: $key"
      aws s3 rm s3://%s/"$key" --endpoint-url %s
    fi
  done
`, backup.Spec.RetentionDays, backup.Spec.RetentionDays, bucketName, endpoint, bucketName, endpoint)

	}
	scratchSizeLimit := backup.Spec.ScratchSizeLimit
	if scratchSizeLimit == "" {
		scratchSizeLimit = "2Gi"
	}
	scratchQuantity, err := resource.ParseQuantity(scratchSizeLimit)
	if err != nil {
		meta.SetStatusCondition(&backup.Status.Conditions, metav1.Condition{
			Type:    "Ready",
			Status:  metav1.ConditionFalse,
			Reason:  "InvalidSpec",
			Message: fmt.Sprintf("Invalid scratchSizeLimit %q: %v", scratchSizeLimit, err),
		})
		r.Status().Update(ctx, &backup)
		return ctrl.Result{}, nil // don't requeue; user must fix the spec
	}
	// FINAL COMBINED SCRIPT
	finalAwsScript := fmt.Sprintf("%s\n%s\necho 'Backup pipeline finalized successfully!'", s3UploadLogic, retentionLogic)

	// DEFINE THE CRONJOB
	cronJobName := fmt.Sprintf("%s-cronjob", backup.Name)
	timeZone := "Asia/Kolkata"
	successfulJobsHistoryLimit := int32(3)
	failedJobsHistoryLimit := int32(3)

	cronJob := &batchv1.CronJob{
		ObjectMeta: metav1.ObjectMeta{
			Name:      cronJobName,
			Namespace: backup.Namespace,
		},
		Spec: batchv1.CronJobSpec{
			Schedule:                   backup.Spec.Schedule,
			TimeZone:                   &timeZone,
			SuccessfulJobsHistoryLimit: &successfulJobsHistoryLimit,
			FailedJobsHistoryLimit:     &failedJobsHistoryLimit,
			JobTemplate: batchv1.JobTemplateSpec{
				Spec: batchv1.JobSpec{
					Template: corev1.PodTemplateSpec{
						Spec: corev1.PodSpec{
							RestartPolicy: corev1.RestartPolicyOnFailure,
							Affinity: &corev1.Affinity{
								PodAffinity: &corev1.PodAffinity{
									RequiredDuringSchedulingIgnoredDuringExecution: []corev1.PodAffinityTerm{
										{
											LabelSelector: &metav1.LabelSelector{
												MatchLabels: map[string]string{
													"app": backup.Spec.TargetApp,
												},
											},
											TopologyKey: "kubernetes.io/hostname",
										},
									},
								},
							},
							// INIT CONTAINER (DATABASE DUMP)
							InitContainers: []corev1.Container{{
								Name:    "database-dump",
								Image:   image, // Dynamically pulls from Blueprint
								Env:     envVars,
								Command: []string{"/bin/sh", "-c"},
								Args:    []string{"set -e\n" + appDumpLogic},
								VolumeMounts: []corev1.VolumeMount{
									{Name: "data-volume", MountPath: mountPath, ReadOnly: true},
									{Name: "scratch-volume", MountPath: "/workspace"},
								},
							}},
							// MAIN CONTAINER (AWS UPLOAD WITH RESOURCES)
							Containers: []corev1.Container{{
								Name:    "s3-upload",
								Image:   "amazon/aws-cli:latest",
								Env:     envVars,
								Command: []string{"/bin/sh", "-c"},
								Args:    []string{finalAwsScript},
								Resources: corev1.ResourceRequirements{
									Requests: corev1.ResourceList{
										corev1.ResourceCPU:    resource.MustParse("100m"),
										corev1.ResourceMemory: resource.MustParse("128Mi"),
									},
									Limits: corev1.ResourceList{
										corev1.ResourceCPU:    resource.MustParse("500m"),
										corev1.ResourceMemory: resource.MustParse("512Mi"),
									},
								},
								VolumeMounts: []corev1.VolumeMount{
									{Name: "scratch-volume", MountPath: "/workspace"},
								},
							}},
							Volumes: []corev1.Volume{
								{
									Name: "data-volume",
									VolumeSource: corev1.VolumeSource{
										PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
											ClaimName: backup.Spec.SourcePVCName,
										},
									},
								},
								{
									Name: "scratch-volume",
									VolumeSource: corev1.VolumeSource{
										EmptyDir: &corev1.EmptyDirVolumeSource{
											SizeLimit: &scratchQuantity,
										},
									},
								},
							},
						},
					},
				},
			},
		},
	}

	if err := ctrl.SetControllerReference(&backup, cronJob, r.Scheme); err != nil {
		return ctrl.Result{}, err
	}

	// MANAGE THE CRONJOB LIFECYCLE
	var existingCronJob batchv1.CronJob
	err = r.Get(ctx, client.ObjectKey{Name: cronJobName, Namespace: backup.Namespace}, &existingCronJob)

	if err != nil && apierrors.IsNotFound(err) {
		logger.Info("Creating a new CronJob", "CronJob.Namespace", cronJob.Namespace, "CronJob.Name", cronJob.Name)
		if err = r.Create(ctx, cronJob); err != nil {
			return ctrl.Result{}, err
		}
	} else if err == nil {
		// Update only if significant parts changed
		if existingCronJob.Spec.Schedule != cronJob.Spec.Schedule ||
			existingCronJob.Spec.JobTemplate.Spec.Template.Spec.Containers[0].Image != cronJob.Spec.JobTemplate.Spec.Template.Spec.Containers[0].Image {

			existingCronJob.Spec = cronJob.Spec
			if err = r.Update(ctx, &existingCronJob); err != nil {
				return ctrl.Result{}, err
			}
			logger.Info("CronJob spec updated successfully")
		}
	} else {
		return ctrl.Result{}, err
	}

	// FINALIZE STATUS
	meta.SetStatusCondition(&backup.Status.Conditions, metav1.Condition{
		Type:    "Ready",
		Status:  metav1.ConditionTrue,
		Reason:  "CronJobSynced",
		Message: "Backup CronJob is provisioned and active",
	})
	if err := r.Status().Update(ctx, &backup); err != nil {
		logger.Error(err, "Failed to update Ready status")
		return ctrl.Result{}, err
	}

	return ctrl.Result{}, nil
}

func (r *BackupReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&corev1alpha1.Backup{}).
		Owns(&batchv1.CronJob{}).
		Complete(r)
}

// GARAGE ADMIN API TYPES
type GarageKeyRequest struct {
	Name string `json:"name"`
}

type GarageKeyResponse struct {
	AccessKeyId     string `json:"accessKeyId"`
	SecretAccessKey string `json:"secretAccessKey"`
}

type GarageBucketRequest struct {
	GlobalAlias string `json:"globalAlias"`
}

type GarageBucketResponse struct {
	ID          string `json:"id"`
	GlobalAlias string `json:"globalAlias"`
}

type GarageAllowRequest struct {
	BucketId    string `json:"bucketId"`
	AccessKeyId string `json:"accessKeyId"`
	Permissions struct {
		Read  bool `json:"read"`
		Write bool `json:"write"`
		Owner bool `json:"owner"`
	} `json:"permissions"`
}

func (r *BackupReconciler) provisionGarageResources(
	ctx context.Context,
	namespace, secretName, bucketName string,
) error {
	logger := log.FromContext(ctx)

	adminEndpoint := os.Getenv("GARAGE_ADMIN_ENDPOINT")
	adminToken := os.Getenv("GARAGE_ADMIN_TOKEN")

	if adminEndpoint == "" || adminToken == "" {
		return fmt.Errorf("GARAGE_ADMIN_ENDPOINT or GARAGE_ADMIN_TOKEN env vars missing")
	}

	httpClient := &http.Client{}

	doRequest := func(method, path string, body interface{}) ([]byte, error) {
		var reqBody io.Reader
		if body != nil {
			jsonData, err := json.Marshal(body)
			if err != nil {
				return nil, fmt.Errorf("marshal error: %w", err)
			}

			logger.Info("Preparing request", "method", method, "path", path, "body", string(jsonData))

			reqBody = bytes.NewBuffer(jsonData)
		} else {
			logger.Info("Preparing request", "method", method, "path", path, "body", "none")
		}

		req, err := http.NewRequest(method, adminEndpoint+path, reqBody)
		if err != nil {
			return nil, fmt.Errorf("request build error: %w", err)
		}
		req.Header.Set("Authorization", "Bearer "+adminToken)
		req.Header.Set("Content-Type", "application/json")

		resp, err := httpClient.Do(req)
		if err != nil {
			return nil, err
		}
		defer resp.Body.Close()

		respData, _ := io.ReadAll(resp.Body)
		if resp.StatusCode == 409 {
			return respData, fmt.Errorf("409_CONFLICT")
		}
		if resp.StatusCode >= 400 {
			return nil, fmt.Errorf("garage API error %d: %s", resp.StatusCode, string(respData))
		}
		return respData, nil
	}

	// Create bucket
	var bucketID string

	bucketData, err := doRequest("POST", "/v1/bucket",
		GarageBucketRequest{GlobalAlias: bucketName})
	if err != nil {
		if err.Error() == "409_CONFLICT" {
			logger.Info("Bucket exists, fetching ID by alias", "bucket", bucketName)
			bucketData, err = doRequest("GET",
				fmt.Sprintf("/v1/bucket?alias=%s", bucketName), nil)
			if err != nil {
				return fmt.Errorf("failed to fetch existing bucket: %w", err)
			}
			var bucketArray []GarageBucketResponse
			if err := json.Unmarshal(bucketData, &bucketArray); err != nil || len(bucketArray) == 0 {
				return fmt.Errorf("failed to parse bucket list: %s", string(bucketData))
			}
			bucketID = bucketArray[0].ID
		} else {
			return fmt.Errorf("failed to create bucket: %w", err)
		}
	} else {
		var bucketResp GarageBucketResponse
		if err := json.Unmarshal(bucketData, &bucketResp); err != nil {
			return fmt.Errorf("failed to parse bucket response: %w", err)
		}
		bucketID = bucketResp.ID
	}

	if bucketID == "" {
		return fmt.Errorf("bucket ID is empty after provisioning")
	}
	logger.Info("Bucket ready", "bucketID", bucketID)

	// Create key (idempotent)
	keyData, err := doRequest("POST", "/v1/key",
		GarageKeyRequest{Name: secretName})
	if err != nil {
		if err.Error() == "409_CONFLICT" {
			// Key exists — delete and recreate to get fresh credentials
			logger.Info("Key exists, deleting and recreating", "key", secretName)
			listData, _ := doRequest("GET", "/v1/key?list", nil)
			var keysList []struct {
				Name        string `json:"name"`
				AccessKeyId string `json:"accessKeyId"`
			}
			if err := json.Unmarshal(listData, &keysList); err == nil {
				for _, k := range keysList {
					if k.Name == secretName {
						doRequest("DELETE",
							fmt.Sprintf("/v1/key?accessKeyId=%s", k.AccessKeyId), nil)
						break
					}
				}
			}
			keyData, err = doRequest("POST", "/v1/key",
				GarageKeyRequest{Name: secretName})
			if err != nil {
				return fmt.Errorf("failed to recreate key: %w", err)
			}
		} else {
			return fmt.Errorf("failed to create key: %w", err)
		}
	}

	var keyResp GarageKeyResponse
	if err := json.Unmarshal(keyData, &keyResp); err != nil {
		return fmt.Errorf("failed to parse key response: %w", err)
	}
	if keyResp.AccessKeyId == "" || keyResp.SecretAccessKey == "" {
		return fmt.Errorf("empty credentials in key response: %s", string(keyData))
	}
	logger.Info("Key ready", "accessKeyId", keyResp.AccessKeyId)

	// Grant permissions
	allowReq := GarageAllowRequest{
		BucketId:    bucketID,
		AccessKeyId: keyResp.AccessKeyId,
	}
	allowReq.Permissions.Read = true
	allowReq.Permissions.Write = true
	allowReq.Permissions.Owner = true

	if _, err := doRequest("POST", "/v1/bucket/allow", allowReq); err != nil {
		return fmt.Errorf("failed to grant permissions: %w", err)
	}
	logger.Info("Permissions granted", "bucketID", bucketID, "key", keyResp.AccessKeyId)

	// Store in Kubernetes Secret
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      secretName,
			Namespace: namespace,
		},
		StringData: map[string]string{
			"AWS_ACCESS_KEY_ID":     keyResp.AccessKeyId,
			"AWS_SECRET_ACCESS_KEY": keyResp.SecretAccessKey,
		},
	}
	if err := r.Create(ctx, secret); err != nil && !apierrors.IsAlreadyExists(err) {
		return fmt.Errorf("failed to create K8s secret: %w", err)
	}

	return nil
}
