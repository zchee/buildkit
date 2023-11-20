package gcs

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"time"

	"cloud.google.com/go/storage"
	"github.com/containerd/containerd/content"
	"github.com/moby/buildkit/cache/remotecache"
	v1 "github.com/moby/buildkit/cache/remotecache/v1"
	"github.com/moby/buildkit/session"
	"github.com/moby/buildkit/solver"
	"github.com/moby/buildkit/util/compression"
	"github.com/moby/buildkit/util/progress"
	digest "github.com/opencontainers/go-digest"
	"github.com/pkg/errors"
)

// ResolveCacheExporterFunc for "gcs" cache exporter.
func ResolveCacheExporterFunc() remotecache.ResolveCacheExporterFunc {
	return func(ctx context.Context, g session.Group, attrs map[string]string) (remotecache.Exporter, error) {
		config, err := getConfig(attrs)
		if err != nil {
			return nil, errors.WithMessage(err, "failed to create azblob config")
		}

		gcsClient, err := newGCSClient(ctx, config)
		if err != nil {
			return nil, errors.WithMessage(err, "failed to create container client")
		}

		cc := v1.NewCacheChains()
		return &exporter{
			CacheExporterTarget: cc,
			chains:              cc,
			client:              gcsClient,
			config:              config,
		}, nil
	}
}

type exporter struct {
	solver.CacheExporterTarget

	chains *v1.CacheChains
	client *gcsClient
	config *Config
}

var _ remotecache.Exporter = (*exporter)(nil)

func (ce *exporter) Name() string {
	return "exporting cache to Googgle Cloud Storage"
}

func (ce *exporter) Finalize(ctx context.Context) (map[string]string, error) {
	cacheConfig, descs, err := ce.chains.Marshal(ctx)
	if err != nil {
		return nil, err
	}

	for i, l := range cacheConfig.Layers {
		dgstPair, ok := descs[l.Blob]
		if !ok {
			return nil, errors.Errorf("missing blob %s", l.Blob)
		}
		if dgstPair.Descriptor.Annotations == nil {
			return nil, errors.Errorf("invalid descriptor without annotations")
		}

		v, ok := dgstPair.Descriptor.Annotations["containerd.io/uncompressed"]
		if !ok {
			return nil, errors.Errorf("invalid descriptor without uncompressed annotation")
		}
		diffID, err := digest.Parse(v)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to parse uncompressed annotation")
		}

		exists, err := ce.client.isObjectExists(ctx, ce.config.Bucket, ce.config.Object)
		if err != nil {
			return nil, err
		}

		if !exists {
			layerDone := progress.OneOff(ctx, fmt.Sprintf("writing layer %s", l.Blob))
			ra, err := dgstPair.Provider.ReaderAt(ctx, dgstPair.Descriptor)
			if err != nil {
				err = errors.Wrapf(err, "failed to get reader for %s", dgstPair.Descriptor.Digest)
				return nil, layerDone(err)
			}
			key := ce.client.blobKey(dgstPair.Descriptor.Digest)
			if err := ce.uploadBlobIfNotExists(ctx, key, content.NewReader(ra)); err != nil {
				return nil, layerDone(err)
			}
			layerDone(nil)
		}
		// logrus.Debugf("layers %s exists = %t", ce.client.blobKey(dgstPair.Descriptor.Digest), exists)

		la := &v1.LayerAnnotations{
			DiffID:    diffID,
			Size:      dgstPair.Descriptor.Size,
			MediaType: dgstPair.Descriptor.MediaType,
		}
		if v, ok := dgstPair.Descriptor.Annotations["buildkit/createdat"]; ok {
			var t time.Time
			if err := (&t).UnmarshalText([]byte(v)); err != nil {
				return nil, err
			}
			la.CreatedAt = t.UTC()
		}
		cacheConfig.Layers[i].Annotations = la
	}

	data, err := json.Marshal(cacheConfig)
	if err != nil {
		return nil, errors.Wrap(err, "failed to marshal config")
	}

	for _, name := range ce.config.Names {
		if err := ce.uploadManifest(ctx, ce.client.manifestKey(name), bytesToReadSeekCloser(data)); err != nil {
			return nil, errors.Wrapf(err, "error writing manifest %s", name)
		}
	}

	return nil, nil
}

func (ce *exporter) uploadManifest(ctx context.Context, manifestKey string, r io.ReadSeekCloser) error {
	defer r.Close()

	bh := ce.client.Bucket(ce.config.Bucket)

	ctx, cancel := context.WithTimeout(ctx, time.Minute*5)
	defer cancel()

	if _, err := bh.Update(ctx, storage.BucketAttrsToUpdate{}); err != nil {
		return errors.Wrapf(err, "failed to upload blob %s: %v", manifestKey, err)
	}
	// if _, err := bh.Upload(ctx, r, &azblob.BlockBlobUploadOptions{}); err != nil {
	// 	return errors.Wrapf(err, "failed to upload blob %s: %v", manifestKey, err)
	// }

	return nil
}

// For uploading manifests, use the Upload API which follows "last writer wins" sematics
// This is slightly slower than UploadStream call but is safe to call concurrently from multiple threads. Refer to:
// https://github.com/Azure/azure-sdk-for-go/issues/18490#issuecomment-1170806877
// func (ce *exporter) uploadManifest(ctx context.Context, manifestKey string, reader io.ReadSeekCloser) error {
// 	defer reader.Close()
// 	blobClient, err := ce.containerClient.NewBlockBlobClient(manifestKey)
// 	if err != nil {
// 		return errors.Wrap(err, "error creating container client")
// 	}
//
// 	ctx, cnclFn := context.WithTimeout(ctx, time.Minute*5)
// 	defer cnclFn()
//
// 	_, err = blobClient.Upload(ctx, reader, &azblob.BlockBlobUploadOptions{})
// 	if err != nil {
// 		return errors.Wrapf(err, "failed to upload blob %s: %v", manifestKey, err)
// 	}
//
// 	return nil
// }

// For uploading blobs, use the UploadStream with access conditions which state that only upload if the blob
// does not already exist. Since blobs are content addressable, this is the right thing to do for blobs and it gives
// a performance improvement over the Upload API used for uploading manifests.
// func (ce *exporter) uploadBlobIfNotExists(ctx context.Context, blobKey string, reader io.Reader) error {
// 	blobClient, err := ce.containerClient.NewBlockBlobClient(blobKey)
// 	if err != nil {
// 		return errors.Wrap(err, "error creating container client")
// 	}
//
// 	uploadCtx, cnclFn := context.WithTimeout(ctx, time.Minute*5)
// 	defer cnclFn()
//
// 	// Only upload if the blob doesn't exist
// 	eTagAny := azblob.ETagAny
// 	_, err = blobClient.UploadStream(uploadCtx, reader, azblob.UploadStreamOptions{
// 		BufferSize: IOChunkSize,
// 		MaxBuffers: IOConcurrency,
// 		BlobAccessConditions: &azblob.BlobAccessConditions{
// 			ModifiedAccessConditions: &azblob.ModifiedAccessConditions{
// 				IfNoneMatch: &eTagAny,
// 			},
// 		},
// 	})
//
// 	if err == nil {
// 		return nil
// 	}
//
// 	var se *azblob.StorageError
// 	if errors.As(err, &se) && se.ErrorCode == azblob.StorageErrorCodeBlobAlreadyExists {
// 		return nil
// 	}
//
// 	return errors.Wrapf(err, "failed to upload blob %s: %v", blobKey, err)
// }

func (ce *exporter) Config() remotecache.Config {
	return remotecache.Config{
		Compression: compression.New(compression.Default),
	}
}

var _ io.ReadSeekCloser = &readSeekCloser{}

type readSeekCloser struct {
	io.Reader
	io.Seeker
	io.Closer
}

func bytesToReadSeekCloser(dt []byte) io.ReadSeekCloser {
	bytesReader := bytes.NewReader(dt)
	return &readSeekCloser{
		Reader: bytesReader,
		Seeker: bytesReader,
		Closer: io.NopCloser(bytesReader),
	}
}
