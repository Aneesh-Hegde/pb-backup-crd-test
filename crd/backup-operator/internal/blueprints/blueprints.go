package blueprints

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

type BlueprintDef struct {
	Name   string
	Image  string
	Script string
}

// Blueprints defines the default backup logic for supported database engines.
// All scripts dump their final output to /workspace/dump.archive for the main AWS container to process.
var Blueprints = []BlueprintDef{
	{
		Name:  "backup-blueprint-sqlite",
		Image: "keinos/sqlite3:latest",
		Script: `echo "Initiating safe SQLite and full volume backup..."

DB_FILENAME="${SQLITE_FILE:-db.sqlite3}"
TARGET_DB="$MOUNT_PATH/$DB_FILENAME"

if [ ! -f "$TARGET_DB" ]; then
  echo "Error: SQLite database not found at $TARGET_DB"
  exit 1
fi

echo "Preparing staging area..."
mkdir -p /workspace/staging

cp -R $MOUNT_PATH/. /workspace/staging/

rm -f "/workspace/staging/$DB_FILENAME"*

echo "Executing safe SQLite hot-copy..."
sqlite3 "$TARGET_DB" ".backup '/workspace/staging/$DB_FILENAME'"

echo "Compressing final archive..."
tar -czf /workspace/dump.archive -C /workspace/staging .

rm -rf /workspace/staging
echo "SQLite and volume backup completed successfully."
`,
	},
	{
		Name:  "backup-blueprint-default",
		Image: "busybox:latest",
		Script: `echo "Initiating standard flat-file volume backup..."

# Compress the entire mounted directory into a tarball
tar -czf /workspace/dump.archive -C $MOUNT_PATH .

echo "Volume tarball created successfully."
`,
	},
	{
		Name:  "backup-blueprint-mongodb",
		Image: "mongo:6.0",
		Script: `echo "Initiating MongoDB archive backup..."

if [ -z "$MONGO_HOST" ]; then
  echo "Error: Missing MONGO_HOST environment variable."
  exit 1
fi

# mongodump supports native gzipping to a single archive file
mongodump --uri="$MONGO_HOST" --archive=/workspace/dump.archive --gzip

echo "MongoDB dump completed successfully."
`,
	},
	{
		Name:  "backup-blueprint-mysql",
		Image: "mysql:8.0",
		Script: `echo "Initiating MySQL/MariaDB logical backup..."

if [ -z "$MYSQL_HOST" ] || [ -z "$MYSQL_USER" ] || [ -z "$MYSQL_PASSWORD" ] || [ -z "$MYSQL_DATABASE" ]; then
  echo "Error: Missing required database environment variables."
  exit 1
fi

# Execute the dump and compress on the fly
mysqldump -h $MYSQL_HOST -u $MYSQL_USER -p$MYSQL_PASSWORD $MYSQL_DATABASE | gzip > /workspace/dump.archive

echo "MySQL dump completed successfully."
`,
	},
	{
		Name:  "backup-blueprint-postgres",
		Image: "postgres:15",
		Script: `echo "Initiating PostgreSQL logical backup..."

if [ -z "$POSTGRES_HOST" ] || [ -z "$POSTGRES_USER" ] || [ -z "$POSTGRES_PASSWORD" ] || [ -z "$POSTGRES_DB" ]; then
  echo "Error: Missing required database environment variables."
  exit 1
fi

# Execute the dump and compress on the fly
PGPASSWORD=$POSTGRES_PASSWORD pg_dump -h $POSTGRES_HOST -U $POSTGRES_USER $POSTGRES_DB | gzip > /workspace/dump.archive

echo "PostgreSQL dump completed successfully."
`,
	},
	{
		Name:  "backup-blueprint-redis",
		Image: "redis:7.0",
		Script: `echo "Initiating Redis memory snapshot..."

if [ -z "$REDIS_HOST" ]; then
  echo "Error: Missing REDIS_HOST environment variable."
  exit 1
fi

# Force Redis to save current memory state to disk synchronously
redis-cli -h $REDIS_HOST SAVE

# Compress the resulting RDB file
gzip -c $MOUNT_PATH/dump.rdb > /workspace/dump.archive

echo "Redis snapshot completed successfully."
`,
	},
}

// EnsureBlueprints iterates through the defined Blueprints and applies them as ConfigMaps
// in the "garage" namespace for the controller to use dynamically.
func EnsureBlueprints(ctx context.Context, c client.Client) error {
	logger := log.FromContext(ctx)
	for _, bp := range Blueprints {
		cm := &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:      bp.Name,
				Namespace: "garage",
			},
			Data: map[string]string{
				"image":     bp.Image,
				"backup.sh": bp.Script,
			},
		}

		err := c.Create(ctx, cm)
		if err != nil {
			if apierrors.IsAlreadyExists(err) {
				// Must fetch first to get ResourceVersion for the update
				existing := &corev1.ConfigMap{}
				if err := c.Get(ctx, client.ObjectKeyFromObject(cm), existing); err != nil {
					return fmt.Errorf("failed to fetch existing blueprint %s: %w", bp.Name, err)
				}
				existing.Data = cm.Data
				if err := c.Update(ctx, existing); err != nil {
					return fmt.Errorf("failed to update blueprint %s: %w", bp.Name, err)
				}
				logger.Info("Blueprint updated", "Blueprint", bp.Name)
				continue
			}
			return fmt.Errorf("failed to create blueprint %s: %w", bp.Name, err)
		}
		logger.Info("Blueprint provisioned", "Blueprint", bp.Name)
	}
	return nil
}
