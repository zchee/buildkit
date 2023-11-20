package gcs

import (
	"os"
	"strings"

	"github.com/pkg/errors"
)

const (
	attrEndpointURL     = "endpoint_url"
	attrCredentials     = "credentials"
	attrBucket          = "bucket"
	attrRegion          = "region"
	attrObject          = "object"
	attrPrefix          = "prefix"
	attrBlobsPrefix     = "blobs_prefix"
	attrManifestsPrefix = "manifests_prefix"
	attrName            = "name"
)

type Config struct {
	EndpointURL     string
	Credentials     string
	Bucket          string
	Region          string
	Object          string
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
			return nil, errors.New("either ${BUILDKIT_GCS_BUCKET} or bucket attribute is required for gcs cache")
		}
	}

	region, ok := attrs[attrRegion]
	if !ok {
		region, ok = os.LookupEnv("BUILDKIT_GCS_REGION")
		if !ok {
			return nil, errors.New("either ${BUILDKIT_GCS_REGION} or region attribute is required for gcs cache")
		}
	}

	object, ok := attrs[attrObject]
	if !ok {
		region, ok = os.LookupEnv("BUILDKIT_GCS_OBJECT")
		if !ok {
			return nil, errors.New("either ${BUILDKIT_GCS_OBJECT} or object attribute is required for gcs cache")
		}
	}

	prefix, ok := attrs[attrPrefix]
	if !ok {
		prefix, _ = os.LookupEnv("BUILDKIT_GCS_PREFIX")
	}

	blobsPrefix, ok := attrs[attrBlobsPrefix]
	if !ok {
		blobsPrefix = "blobs"
	}

	manifestsPrefix, ok := attrs[attrManifestsPrefix]
	if !ok {
		manifestsPrefix = "manifests"
	}

	names := []string{"buildkit"}
	name, ok := attrs[attrName]
	if ok {
		splittedNames := strings.Split(name, ";")
		if len(splittedNames) > 0 {
			names = splittedNames
		}
	}

	credential := attrs[attrCredentials]

	config := Config{
		Region: region,
		Bucket: bucket,
		Object: object,
		// AccountURL:      accountURLString,
		// AccountName:     accountName,
		// Container:       container,
		Prefix:          prefix,
		Names:           names,
		ManifestsPrefix: manifestsPrefix,
		BlobsPrefix:     blobsPrefix,
		// secretAccessKey: secretAccessKey,
		Credentials: credential,
	}

	return &config, nil
}
