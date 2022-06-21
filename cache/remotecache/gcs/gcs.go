package gcs

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"cloud.google.com/go/storage"
	"github.com/containerd/containerd/content"
	"github.com/moby/buildkit/cache/remotecache"
	v1 "github.com/moby/buildkit/cache/remotecache/v1"
	"github.com/moby/buildkit/session"
	"github.com/moby/buildkit/solver"
	"github.com/moby/buildkit/util/compression"
	"github.com/moby/buildkit/util/progress"
	"github.com/moby/buildkit/worker"
	digest "github.com/opencontainers/go-digest"
	ocispecs "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/pkg/errors"
	"google.golang.org/api/option"
)

const (
	attrBucket       = "bucket"
	attrPrefix       = "prefix"
	attrBlobsPrefix  = "blobs_prefix"
	attrName         = "name"
	attrTouchRefresh = "touch_refresh"
	attrEndpointURL  = "endpoint_url"
	attrCredential   = "credential"
)

type Config struct {
	Bucket       string
	Names        []string
	TouchRefresh time.Duration
	EndpointURL  string
	Credential   string
}

func getConfig(attrs map[string]string) (Config, error) {
	bucket, ok := attrs[attrBucket]
	if !ok {
		bucket, ok = os.LookupEnv("GCS_BUCKET")
		if !ok {
			return Config{}, errors.Errorf("bucket ($GCS_BUCKET) not set for gcs cache")
		}
	}

	names := []string{"buildkit"}
	name, ok := attrs[attrName]
	if ok {
		splittedNames := strings.Split(name, ";")
		if len(splittedNames) > 0 {
			names = splittedNames
		}
	}

	endpointURL := attrs[attrEndpointURL]
	credential := attrs[attrCredential]

	return Config{
		Bucket:      bucket,
		Names:       names,
		EndpointURL: endpointURL,
		Credential:  credential,
	}, nil
}

type exporter struct {
	solver.CacheExporterTarget
	chains    *v1.CacheChains
	gcsClient *gcsClient
	config    Config
}

// ResolveCacheExporterFunc for gcs cache exporter.
func ResolveCacheExporterFunc() remotecache.ResolveCacheExporterFunc {
	return func(ctx context.Context, g session.Group, attrs map[string]string) (remotecache.Exporter, error) {
		config, err := getConfig(attrs)
		if err != nil {
			return nil, err
		}

		gcsClient, err := newGCSClient(ctx, config)
		if err != nil {
			return nil, err
		}
		cc := v1.NewCacheChains()

		return &exporter{
			CacheExporterTarget: cc,
			chains:              cc,
			gcsClient:           gcsClient,
			config:              config,
		}, nil
	}
}

func (e *exporter) Config() remotecache.Config {
	return remotecache.Config{
		Compression: compression.New(compression.Default),
	}
}

func (e *exporter) Finalize(ctx context.Context) (map[string]string, error) {
	cacheConfig, descs, err := e.chains.Marshal(ctx)
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

		key := dgstPair.Descriptor.Digest.String()
		exists, err := e.gcsClient.exists(ctx, key)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to check file presence in cache")
		}
		if exists != nil {
			if time.Since(*exists) > e.config.TouchRefresh {
				err = e.gcsClient.touch(ctx, key)
				if err != nil {
					return nil, errors.Wrapf(err, "failed to touch file")
				}
			}
		} else {
			layerDone := oneOffProgress(ctx, fmt.Sprintf("writing layer %s", l.Blob))
			bytes, err := content.ReadBlob(ctx, dgstPair.Provider, dgstPair.Descriptor)
			if err != nil {
				return nil, layerDone(err)
			}
			if err := e.gcsClient.saveMutable(ctx, key, bytes); err != nil {
				return nil, layerDone(errors.Wrap(err, "error writing layer blob"))
			}

			layerDone(nil)
		}

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

	dt, err := json.Marshal(cacheConfig)
	if err != nil {
		return nil, err
	}

	for _, name := range e.config.Names {
		if err := e.gcsClient.saveMutable(ctx, name, dt); err != nil {
			return nil, errors.Wrapf(err, "error writing manifest: %s", name)
		}
	}
	return nil, nil
}

// ResolveCacheImporterFunc for s3 cache importer.
func ResolveCacheImporterFunc() remotecache.ResolveCacheImporterFunc {
	return func(ctx context.Context, _ session.Group, attrs map[string]string) (remotecache.Importer, ocispecs.Descriptor, error) {
		config, err := getConfig(attrs)
		if err != nil {
			return nil, ocispecs.Descriptor{}, err
		}
		gcsClient, err := newGCSClient(ctx, config)
		if err != nil {
			return nil, ocispecs.Descriptor{}, err
		}
		return &importer{gcsClient, config}, ocispecs.Descriptor{}, nil
	}
}

type importer struct {
	gcsClient *gcsClient
	config    Config
}

func (i *importer) makeDescriptorProviderPair(l v1.CacheLayer) (*v1.DescriptorProviderPair, error) {
	if l.Annotations == nil {
		return nil, errors.Errorf("cache layer with missing annotations")
	}
	if l.Annotations.DiffID == "" {
		return nil, errors.Errorf("cache layer with missing diffid")
	}
	annotations := map[string]string{}
	annotations["containerd.io/uncompressed"] = l.Annotations.DiffID.String()
	if !l.Annotations.CreatedAt.IsZero() {
		txt, err := l.Annotations.CreatedAt.MarshalText()
		if err != nil {
			return nil, err
		}
		annotations["buildkit/createdat"] = string(txt)
	}
	return &v1.DescriptorProviderPair{
		Provider: i.gcsClient,
		Descriptor: ocispecs.Descriptor{
			MediaType:   l.Annotations.MediaType,
			Digest:      l.Blob,
			Size:        l.Annotations.Size,
			Annotations: annotations,
		},
	}, nil
}

func (i *importer) load(ctx context.Context) (*v1.CacheChains, error) {
	var config v1.CacheConfig
	found, err := i.gcsClient.getManifest(ctx, i.config.Names[0], &config)
	if err != nil {
		return nil, err
	}
	if !found {
		return v1.NewCacheChains(), nil
	}

	allLayers := v1.DescriptorProvider{}

	for _, l := range config.Layers {
		dpp, err := i.makeDescriptorProviderPair(l)
		if err != nil {
			return nil, err
		}
		allLayers[l.Blob] = *dpp
	}

	cc := v1.NewCacheChains()
	if err := v1.ParseConfig(config, allLayers, cc); err != nil {
		return nil, err
	}
	return cc, nil
}

func (i *importer) Resolve(ctx context.Context, _ ocispecs.Descriptor, id string, w worker.Worker) (solver.CacheManager, error) {
	cc, err := i.load(ctx)
	if err != nil {
		return nil, err
	}

	keysStorage, resultStorage, err := v1.NewCacheKeyStorage(cc, w)
	if err != nil {
		return nil, err
	}

	return solver.NewCacheManager(ctx, id, keysStorage, resultStorage), nil
}

type readerAt struct {
	ReaderAtCloser
	size int64
}

func (r *readerAt) Size() int64 {
	return r.size
}

func oneOffProgress(ctx context.Context, id string) func(err error) error {
	pw, _, _ := progress.NewFromContext(ctx)
	now := time.Now()
	st := progress.Status{
		Started: &now,
	}
	pw.Write(id, st)
	return func(err error) error {
		now := time.Now()
		st.Completed = &now
		pw.Write(id, st)
		pw.Close()
		return err
	}
}

type gcsClient struct {
	*storage.Client
	bucket string
}

func newGCSClient(ctx context.Context, config Config) (*gcsClient, error) {
	var opts []option.ClientOption
	if credis := config.Credential; credis != "" {
		opts = append(opts, option.WithCredentialsFile(credis))
	}
	if endpoint := config.EndpointURL; endpoint != "" {
		opts = append(opts, option.WithEndpoint(endpoint))
	}

	client, err := storage.NewClient(ctx, opts...)
	if err != nil {
		return nil, errors.Errorf("could not create storage client: %w", err)
	}

	return &gcsClient{
		Client: client,
		bucket: config.Bucket,
	}, nil
}

func (c *gcsClient) getManifest(ctx context.Context, name string, config *v1.CacheConfig) (bool, error) {
	bkt := c.Bucket(c.bucket)
	r, err := bkt.Object(name).NewReader(ctx)
	if err != nil {
		if isNotFound(err) {
			return false, nil
		}
		return false, err
	}
	defer r.Close()

	dec := json.NewDecoder(r)
	if err := dec.Decode(config); err != nil {
		return false, errors.WithStack(err)
	}

	return true, nil
}

func (c *gcsClient) getReader(ctx context.Context, name string) (io.ReadCloser, error) {
	r, err := c.Bucket(c.bucket).Object(name).NewReader(ctx)
	if err != nil {
		return nil, err
	}

	return r, nil
}

func (c *gcsClient) saveMutable(ctx context.Context, name string, value []byte) error {
	w := c.Bucket(c.bucket).Object(name).NewWriter(ctx)
	_, err := w.Write(value)

	return err
}

func (c *gcsClient) exists(ctx context.Context, name string) (*time.Time, error) {
	attr, err := c.Bucket(c.bucket).Object(name).Attrs(ctx)
	if err != nil {
		if isNotFound(err) {
			return nil, nil
		}
		return nil, err
	}

	return &attr.Updated, nil
}

func (c *gcsClient) touch(ctx context.Context, name string) error {
	objSrc := c.Bucket(c.bucket).Object(name)
	copier := c.Bucket(c.bucket).Object(name).CopierFrom(objSrc)

	_, err := copier.Run(ctx)

	return err
}

func (c *gcsClient) ReaderAt(ctx context.Context, desc ocispecs.Descriptor) (content.ReaderAt, error) {
	readerAtCloser := toReaderAtCloser(func(offset int64) (io.ReadCloser, error) {
		return c.getReader(ctx, desc.Digest.String())
	})

	return &readerAt{
		ReaderAtCloser: readerAtCloser,
		size:           desc.Size,
	}, nil
}

func isNotFound(err error) bool {
	return errors.Is(err, storage.ErrObjectNotExist)
}
