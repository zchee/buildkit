package gcs

import (
	"context"
	"path/filepath"
	"time"

	"cloud.google.com/go/storage"
	digest "github.com/opencontainers/go-digest"
	"github.com/pkg/errors"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/option"
)

const (
	ioConcurrency = 4
	ioChunkSize   = 32 * 1024 * 1024
)

type gcsClient struct {
	*storage.Client

	bucket          string
	prefix          string
	blobsPrefix     string
	manifestsPrefix string
}

func newGCSClient(ctx context.Context, config *Config) (*gcsClient, error) {
	opts := []option.ClientOption{
		option.WithScopes(storage.ScopeReadWrite),
	}

	if credsFile := config.Credentials; credsFile != "" {
		opts = append(opts, option.WithCredentialsFile(credsFile))
	} else {
		creds, err := google.DefaultTokenSource(ctx, storage.ScopeReadWrite)
		if err != nil {
			return nil, errors.Wrap(err, "could not find default credentials")
		}
		opts = append(opts, option.WithTokenSource(creds))
	}

	client, err := storage.NewClient(ctx, opts...)
	if err != nil {
		return nil, errors.Wrap(err, "could not create storage client")
	}

	ctx, cancel := context.WithTimeout(ctx, time.Second*60)
	defer cancel()

	// containerClient, err := serviceClient.NewContainerClient(config.Container)
	// if err != nil {
	// 	return nil, errors.Wrap(err, "error creating container client")
	// }

	// _, err = containerClient.GetProperties(ctx, &azblob.ContainerGetPropertiesOptions{})
	// if err == nil {
	// 	return containerClient, nil
	// }

	if errors.Is(err, storage.ErrBucketNotExist) {
		// ctx, cancel := context.WithTimeout(ctx, time.Minute*5)
		// defer cancel()

		// _, err := containerClient.Create(ctx, &azblob.ContainerCreateOptions{})
		// if err != nil {
		// 	return nil, errors.Wrapf(err, "failed to create cache container %s", config.Container)
		// }

		// return containerClient, nil
	}

	// return nil, errors.Wrapf(err, "failed to get properties of cache container %s", config.Container)

	return &gcsClient{
		Client:          client,
		bucket:          config.Bucket,
		prefix:          config.Prefix,
		blobsPrefix:     config.BlobsPrefix,
		manifestsPrefix: config.ManifestsPrefix,
	}, nil
}

func (c *gcsClient) blobKey(digest digest.Digest) string {
	return filepath.Join(c.prefix, c.blobsPrefix, digest.String())
}

func (c *gcsClient) manifestKey(name string) string {
	return filepath.Join(c.prefix, c.manifestsPrefix, name)
}

func (c *gcsClient) isObjectExists(ctx context.Context, bucketKey, objKey string) (bool, error) {
	objHandler := c.Bucket(bucketKey).Object(objKey)

	ctx, cancel := context.WithTimeout(ctx, time.Second*60)
	defer cancel()

	if _, err := objHandler.Attrs(ctx); err != nil {
		if errors.Is(err, storage.ErrObjectNotExist) {
			return false, nil
		}

		return false, errors.Wrap(err, "could not get object attributes")
	}

	return true, nil
}

// func createContainerClient(ctx context.Context, config *Config) (*storage.Client, error) {
// 	var serviceClient *azblob.ServiceClient
// 	if config.secretAccessKey != "" {
// 		sharedKey, err := azblob.NewSharedKeyCredential(config.AccountName, config.secretAccessKey)
// 		if err != nil {
// 			return nil, errors.Wrap(err, "failed to create shared key")
// 		}
// 		serviceClient, err = azblob.NewServiceClientWithSharedKey(config.AccountURL, sharedKey, &azblob.ClientOptions{})
// 		if err != nil {
// 			return nil, errors.Wrap(err, "failed to created service client from shared key")
// 		}
// 	} else {
// 		cred, err := azidentity.NewDefaultAzureCredential(nil)
// 		if err != nil {
// 			return nil, errors.Wrap(err, "failed to create default azure credentials")
// 		}
//
// 		serviceClient, err = azblob.NewServiceClient(config.AccountURL, cred, &azblob.ClientOptions{})
// 		if err != nil {
// 			return nil, errors.Wrap(err, "failed to create service client")
// 		}
// 	}
//
// 	ctx, cnclFn := context.WithTimeout(ctx, time.Second*60)
// 	defer cnclFn()
//
// 	containerClient, err := serviceClient.NewContainerClient(config.Container)
// 	if err != nil {
// 		return nil, errors.Wrap(err, "error creating container client")
// 	}
//
// 	_, err = containerClient.GetProperties(ctx, &azblob.ContainerGetPropertiesOptions{})
// 	if err == nil {
// 		return containerClient, nil
// 	}
//
// 	var se *azblob.StorageError
// 	if errors.As(err, &se) && se.ErrorCode == azblob.StorageErrorCodeContainerNotFound {
// 		ctx, cnclFn := context.WithTimeout(ctx, time.Minute*5)
// 		defer cnclFn()
// 		_, err := containerClient.Create(ctx, &azblob.ContainerCreateOptions{})
// 		if err != nil {
// 			return nil, errors.Wrapf(err, "failed to create cache container %s", config.Container)
// 		}
//
// 		return containerClient, nil
// 	}
//
// 	return nil, errors.Wrapf(err, "failed to get properties of cache container %s", config.Container)
// }
