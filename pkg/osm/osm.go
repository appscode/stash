package osm
import (
	"bytes"
	"net/url"
	"strconv"
	"strings"

	stringz "github.com/appscode/go/strings"
	"github.com/appscode/go/types"
	otx "github.com/appscode/osm/context"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/credentials/stscreds"
	"github.com/aws/aws-sdk-go/aws/session"
	_s3 "github.com/aws/aws-sdk-go/service/s3"
	"github.com/ghodss/yaml"
	"github.com/graymeta/stow"
	"github.com/graymeta/stow/azure"
	gcs "github.com/graymeta/stow/google"
	"github.com/graymeta/stow/s3"
	"github.com/graymeta/stow/swift"
	"github.com/pkg/errors"
	core "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	api "github.com/appscode/stash/apis/stash/v1alpha1"
	cli "github.com/appscode/stash/pkg/cli"
)

const (
	SecretMountPath = "/etc/osm"
)

func NewOSMSecret(client kubernetes.Interface, backend api.Backend) (*core.Secret, error) {
	osmCtx, err := NewOSMContext(client, snapshot.Spec.SnapshotStorageSpec, snapshot.Namespace)
	if err != nil {
		return nil, err
	}
	osmCfg := &otx.OSMConfig{
		CurrentContext: osmCtx.Name,
		Contexts:       []*otx.Context{osmCtx},
	}
	osmBytes, err := yaml.Marshal(osmCfg)
	if err != nil {
		return nil, err
	}
	return &core.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      snapshot.OSMSecretName(),
			Namespace: snapshot.Namespace,
		},
		Data: map[string][]byte{
			"config": osmBytes,
		},
	}, nil
}

func CheckBucketAccess(client kubernetes.Interface, repository *api.Repository, namespace string) error {
	cfg, err := NewOSMContext(client, repository.Spec.Backend, namespace)
	if err != nil {
		return err
	}
	loc, err := stow.Dial(cfg.Provider, cfg.Config)
	if err != nil {
		return err
	}
	c, err := repository.Spec.Backend
	if err != nil {
		return err
	}
	container, err := loc.Container(c)
	if err != nil {
		return err
	}
	r := bytes.NewReader([]byte("CheckBucketAccess"))
	item, err := container.Put(".kubedb", r, r.Size(), nil)
	if err != nil {
		return err
	}
	if err := container.RemoveItem(item.ID()); err != nil {
		return err
	}
	return nil
}

func NewOSMContext(client kubernetes.Interface, repository *api.Repository) (*otx.Context, error) {
	config := make(map[string][]byte)

	if repository.Spec.Backend.StorageSecretName != "" {
		secret, err := client.CoreV1().Secrets(repository.Namespace).Get(repository.Spec.Backend.StorageSecretName, metav1.GetOptions{})
		if err != nil {
			return nil, err
		}
		config = secret.Data
	}

	nc := &otx.Context{
		Name:   "stash",
		Config: stow.ConfigMap{},
	}

	if repository.Spec.Backend.S3 != nil {
		nc.Provider = s3.Kind

		keyID, foundKeyID := config[cli.AWS_ACCESS_KEY_ID]
		key, foundKey := config[cli.AWS_SECRET_ACCESS_KEY]
		if foundKey && foundKeyID {
			nc.Config[s3.ConfigAccessKeyID] = string(keyID)
			nc.Config[s3.ConfigSecretKey] = string(key)
			nc.Config[s3.ConfigAuthType] = "accesskey"
		} else {
			nc.Config[s3.ConfigAuthType] = "iam"
		}
		if strings.HasSuffix(repository.Spec.Backend.S3.Endpoint, ".amazonaws.com") {
			// find region
			var sess *session.Session
			var err error
			if nc.Config[s3.ConfigAuthType] == "iam" {
				sess, err = session.NewSessionWithOptions(session.Options{
					Config: *aws.NewConfig(),
					// Support MFA when authing using assumed roles.
					SharedConfigState:       session.SharedConfigEnable,
					AssumeRoleTokenProvider: stscreds.StdinTokenProvider,
				})
			} else {
				config := &aws.Config{
					Credentials: credentials.NewStaticCredentials(string(keyID), string(key), ""),
					Region:      aws.String("us-east-1"),
				}
				sess, err = session.NewSessionWithOptions(session.Options{
					Config: *config,
					// Support MFA when authing using assumed roles.
					SharedConfigState:       session.SharedConfigEnable,
					AssumeRoleTokenProvider: stscreds.StdinTokenProvider,
				})
			}
			if err != nil {
				return nil, err
			}
			svc := _s3.New(sess)
			out, err := svc.GetBucketLocation(&_s3.GetBucketLocationInput{
				Bucket: types.StringP(repository.Spec.Backend.S3.Bucket),
			})
			nc.Config[s3.ConfigRegion] = stringz.Val(types.String(out.LocationConstraint), "us-east-1")
		} else {
			nc.Config[s3.ConfigEndpoint] = repository.Spec.Backend.S3.Endpoint
			if u, err := url.Parse(repository.Spec.Backend.S3.Endpoint); err == nil {
				nc.Config[s3.ConfigDisableSSL] = strconv.FormatBool(u.Scheme == "http")
			}
		}
		return nc, nil
	} else if repository.Spec.Backend.GCS != nil {
		nc.Provider = gcs.Kind
		nc.Config[gcs.ConfigProjectId] = string(config[cli.GOOGLE_PROJECT_ID])
		nc.Config[gcs.ConfigJSON] = string(config[cli.GOOGLE_SERVICE_ACCOUNT_JSON_KEY])
		return nc, nil
	} else if repository.Spec.Backend.Azure != nil {
		nc.Provider = azure.Kind
		nc.Config[azure.ConfigAccount] = string(config[cli.AZURE_ACCOUNT_NAME])
		nc.Config[azure.ConfigKey] = string(config[cli.AZURE_ACCOUNT_KEY])
		return nc, nil
	} else if repository.Spec.Backend.Swift != nil {
		nc.Provider = swift.Kind
		// https://github.com/restic/restic/blob/master/src/restic/backend/swift/config.go
		for _, val := range []struct {
			stowKey   string
			secretKey string
		}{
			// For keystone v1 authentication
			{swift.ConfigTenantAuthURL, cli.ST_AUTH},
			{swift.ConfigUsername, cli.ST_USER},
			{swift.ConfigKey, cli.ST_KEY},

			// For keystone v2 authentication (some variables are optional)
			{swift.ConfigTenantAuthURL, cli.OS_AUTH_URL},
			{swift.ConfigRegion, cli.OS_REGION_NAME},
			{swift.ConfigUsername, cli.OS_USERNAME},
			{swift.ConfigKey, cli.OS_PASSWORD},
			{swift.ConfigTenantId, cli.OS_TENANT_ID},
			{swift.ConfigTenantName, cli.OS_TENANT_NAME},

			// For keystone v3 authentication (some variables are optional)
			{swift.ConfigDomain, cli.OS_USER_DOMAIN_NAME},
			{swift.ConfigTenantName, cli.OS_PROJECT_NAME},
			{swift.ConfigTenantDomain, cli.OS_PROJECT_DOMAIN_NAME},

			// Manual authentication
			{swift.ConfigStorageURL, cli.OS_STORAGE_URL},
			{swift.ConfigAuthToken, cli.OS_AUTH_TOKEN},
		} {
			if _, exists := nc.Config.Config(val.stowKey); !exists {
				nc.Config[val.stowKey] = string(config[val.secretKey])
			}
		}
		return nc, nil
	}
	return nil, errors.New("no storage provider is configured")
}
