package contentutil

import (
	"bytes"
	"context"
	"io"
	"testing"

	"github.com/containerd/containerd/content"
	"github.com/containerd/containerd/errdefs"
	digest "github.com/opencontainers/go-digest"
	ocispecs "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
)

func TestReadWrite(t *testing.T) {
	t.Parallel()
	ctx := context.TODO()

	b := NewBuffer()

	err := content.WriteBlob(ctx, b, "foo", bytes.NewBuffer([]byte("foo0")), ocispecs.Descriptor{Size: -1})
	require.NoError(t, err)

	err = content.WriteBlob(ctx, b, "foo", bytes.NewBuffer([]byte("foo1")), ocispecs.Descriptor{Size: 4})
	require.NoError(t, err)

	err = content.WriteBlob(ctx, b, "foo", bytes.NewBuffer([]byte("foo2")), ocispecs.Descriptor{Size: 3})
	require.Error(t, err)

	err = content.WriteBlob(ctx, b, "foo", bytes.NewBuffer([]byte("foo3")), ocispecs.Descriptor{Size: -1, Digest: digest.FromBytes([]byte("foo4"))})
	require.Error(t, err)

	err = content.WriteBlob(ctx, b, "foo", bytes.NewBuffer([]byte("foo4")), ocispecs.Descriptor{Size: -1, Digest: digest.FromBytes([]byte("foo4"))})
	require.NoError(t, err)

	dt, err := content.ReadBlob(ctx, b, ocispecs.Descriptor{Digest: digest.FromBytes([]byte("foo1"))})
	require.NoError(t, err)
	require.Equal(t, string(dt), "foo1")

	_, err = content.ReadBlob(ctx, b, ocispecs.Descriptor{Digest: digest.FromBytes([]byte("foo3"))})
	require.Error(t, err)
	require.Equal(t, true, errors.Is(err, errdefs.ErrNotFound))
}

func TestReaderAt(t *testing.T) {
	t.Parallel()
	ctx := context.TODO()

	b := NewBuffer()

	err := content.WriteBlob(ctx, b, "foo", bytes.NewBuffer([]byte("foobar")), ocispecs.Descriptor{Size: -1})
	require.NoError(t, err)

	rdr, err := b.ReaderAt(ctx, ocispecs.Descriptor{Digest: digest.FromBytes([]byte("foobar"))})
	require.NoError(t, err)

	require.Equal(t, int64(6), rdr.Size())

	buf := make([]byte, 3)

	n, err := rdr.ReadAt(buf, 1)
	require.NoError(t, err)
	require.Equal(t, "oob", string(buf[:n]))

	buf = make([]byte, 7)

	n, err = rdr.ReadAt(buf, 3)
	require.Error(t, err)
	require.Equal(t, err, io.EOF)
	require.Equal(t, "bar", string(buf[:n]))
}
