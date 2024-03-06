package gcs

import (
	"context"
	"os"
	"path/filepath"
	"strings"

	"cloud.google.com/go/storage"
	"github.com/pkg/errors"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/option"
)

const (
	attrBucket          = "bucket"
	attrRegion          = "region"
	attrCredentials     = "credentials"
	attrPrefix          = "prefix"
	attrManifestsPrefix = "manifests_prefix"
	attrBlobsPrefix     = "blobs_prefix"
	attrName            = "name"
)

type Config struct {
	Bucket          string
	Region          string
	Credentials     string
	Prefix          string
	ManifestsPrefix string
	BlobsPrefix     string
	Names           []string
}

func getConfig(attrs map[string]string) (*Config, error) {
	bucket, ok := attrs[attrBucket]
	if !ok {
		bucket, ok = os.LookupEnv("BUILDKIT_GCS_BUCKET")
		if !ok {
			return nil, errors.Errorf("bucket ($GCS_BUCKET) not set for s3 cache")
		}
	}

	region, ok := attrs[attrRegion]
	if !ok {
		region, ok = os.LookupEnv("BUILDKIT_GCS_REGION")
		if !ok {
			return nil, errors.Errorf("region ($GCS_REGION) not set for s3 cache")
		}
	}

	credentials, ok := attrs[attrCredentials]
	if !ok {
		credentials, _ = os.LookupEnv("BUILDKIT_GCS_CREDENTIALS")
	}

	prefix := attrs[attrPrefix]
	if !ok {
		prefix, _ = os.LookupEnv("BUILDKIT_GCS_PREFIX")
	}

	manifestsPrefix, ok := attrs[attrManifestsPrefix]
	if !ok {
		manifestsPrefix = "manifests"
	}

	blobsPrefix, ok := attrs[attrBlobsPrefix]
	if !ok {
		blobsPrefix = "blobs"
	}

	names := []string{"buildkit"}
	name, ok := attrs[attrName]
	if ok {
		splittedNames := strings.Split(name, ";")
		if len(splittedNames) > 0 {
			names = splittedNames
		}
	}

	return &Config{
		Bucket:          bucket,
		Region:          region,
		Credentials:     credentials,
		Prefix:          prefix,
		ManifestsPrefix: manifestsPrefix,
		BlobsPrefix:     blobsPrefix,
		Names:           names,
	}, nil
}

type gcsClient struct {
	gcsClient *storage.Client

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
		return nil, errors.Wrap(err, "failed to create gcs client")
	}

	return &gcsClient{
		gcsClient:       client,
		bucket:          config.Bucket,
		prefix:          config.Prefix,
		blobsPrefix:     config.BlobsPrefix,
		manifestsPrefix: config.ManifestsPrefix,
	}, nil
}

func manifestKey(config *Config, name string) string {
	key := filepath.Join(config.Prefix, config.ManifestsPrefix, name)
	return key
}

func blobKey(config *Config, digest string) string {
	key := filepath.Join(config.Prefix, config.BlobsPrefix, digest)
	return key
}

func blobExists(ctx context.Context, gcsClient *storage.Client, blobKey string) (bool, error) {
	// gcsClient.Bucket(blobKey).If()
	// blobClient, err := containerClient.NewBlobClient(blobKey)
	// if err != nil {
	// 	return false, errors.Wrap(err, "error creating blob client")
	// }
	//
	// ctx, cnclFn := context.WithCancelCause(ctx)
	// ctx, _ = context.WithTimeoutCause(ctx, time.Second*60, errors.WithStack(context.DeadlineExceeded))
	// defer cnclFn(errors.WithStack(context.Canceled))
	// _, err = blobClient.GetProperties(ctx, &azblob.BlobGetPropertiesOptions{})
	// if err == nil {
	// 	return true, nil
	// }
	//
	// var se *azblob.StorageError
	// if errors.As(err, &se) && se.ErrorCode == azblob.StorageErrorCodeBlobNotFound {
	// 	return false, nil
	// }

	return false, errors.Wrapf(nil, "failed to check blob %s existence", blobKey)
}

// type s3Client struct {
// 	*s3.Client
// 	*manager.Uploader
// 	bucket          string
// 	prefix          string
// 	blobsPrefix     string
// 	manifestsPrefix string
// }

// func newS3Client(ctx context.Context, config Config) (*s3Client, error) {
// 	cfg, err := aws_config.LoadDefaultConfig(ctx, aws_config.WithRegion(config.Region))
// 	if err != nil {
// 		return nil, errors.Errorf("Unable to load AWS SDK config, %v", err)
// 	}
// 	client := s3.NewFromConfig(cfg, func(options *s3.Options) {
// 		if config.AccessKeyID != "" && config.SecretAccessKey != "" {
// 			options.Credentials = credentials.NewStaticCredentialsProvider(config.AccessKeyID, config.SecretAccessKey, config.SessionToken)
// 		}
// 		if config.EndpointURL != "" {
// 			options.UsePathStyle = config.UsePathStyle
// 			options.BaseEndpoint = aws.String(config.EndpointURL)
// 		}
// 	})
//
// 	return &s3Client{
// 		Client:          client,
// 		Uploader:        manager.NewUploader(client),
// 		bucket:          config.Bucket,
// 		prefix:          config.Prefix,
// 		blobsPrefix:     config.BlobsPrefix,
// 		manifestsPrefix: config.ManifestsPrefix,
// 	}, nil
// }

// func (s3Client *s3Client) getManifest(ctx context.Context, key string, config *v1.CacheConfig) (bool, error) {
// 	input := &s3.GetObjectInput{
// 		Bucket: &s3Client.bucket,
// 		Key:    &key,
// 	}
//
// 	output, err := s3Client.GetObject(ctx, input)
// 	if err != nil {
// 		if isNotFound(err) {
// 			return false, nil
// 		}
// 		return false, err
// 	}
// 	defer output.Body.Close()
//
// 	decoder := json.NewDecoder(output.Body)
// 	if err := decoder.Decode(config); err != nil {
// 		return false, errors.WithStack(err)
// 	}
//
// 	return true, nil
// }
//
// func (s3Client *s3Client) getReader(ctx context.Context, key string) (io.ReadCloser, error) {
// 	input := &s3.GetObjectInput{
// 		Bucket: &s3Client.bucket,
// 		Key:    &key,
// 	}
//
// 	output, err := s3Client.GetObject(ctx, input)
// 	if err != nil {
// 		return nil, err
// 	}
// 	return output.Body, nil
// }
//
// func (s3Client *s3Client) saveMutable(ctx context.Context, key string, value []byte) error {
// 	input := &s3.PutObjectInput{
// 		Bucket: &s3Client.bucket,
// 		Key:    &key,
//
// 		Body: bytes.NewReader(value),
// 	}
// 	_, err := s3Client.Upload(ctx, input)
// 	return err
// }
//
// func (s3Client *s3Client) exists(ctx context.Context, key string) (*time.Time, error) {
// 	input := &s3.HeadObjectInput{
// 		Bucket: &s3Client.bucket,
// 		Key:    &key,
// 	}
//
// 	head, err := s3Client.HeadObject(ctx, input)
// 	if err != nil {
// 		if isNotFound(err) {
// 			return nil, nil
// 		}
// 		return nil, err
// 	}
// 	return head.LastModified, nil
// }
//
// func (s3Client *s3Client) touch(ctx context.Context, key string) error {
// 	copySource := fmt.Sprintf("%s/%s", s3Client.bucket, key)
// 	cp := &s3.CopyObjectInput{
// 		Bucket:            &s3Client.bucket,
// 		CopySource:        &copySource,
// 		Key:               &key,
// 		Metadata:          map[string]string{"updated-at": time.Now().String()},
// 		MetadataDirective: "REPLACE",
// 	}
//
// 	_, err := s3Client.CopyObject(ctx, cp)
//
// 	return err
// }
//
// func (s3Client *s3Client) ReaderAt(ctx context.Context, desc ocispecs.Descriptor) (content.ReaderAt, error) {
// 	readerAtCloser := toReaderAtCloser(func(offset int64) (io.ReadCloser, error) {
// 		return s3Client.getReader(ctx, s3Client.blobKey(desc.Digest))
// 	})
// 	return &readerAt{ReaderAtCloser: readerAtCloser, size: desc.Size}, nil
// }
//
// func (s3Client *s3Client) manifestKey(name string) string {
// 	return s3Client.prefix + s3Client.manifestsPrefix + name
// }
//
// func (s3Client *s3Client) blobKey(dgst digest.Digest) string {
// 	return s3Client.prefix + s3Client.blobsPrefix + dgst.String()
// }
//
// func isNotFound(err error) bool {
// 	var nf *s3types.NotFound
// 	var nsk *s3types.NoSuchKey
// 	return errors.As(err, &nf) || errors.As(err, &nsk)
// }
