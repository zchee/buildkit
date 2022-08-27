# syntax=docker/dockerfile-upstream:master

ARG FAKE_GCS_VERSION=1.39

FROM fsouza/fake-gcs-server:${FAKE_GCS_VERSION} AS fake-gcs-server
FROM moby/buildkit AS buildkit

FROM gcr.io/google.com/cloudsdktool/cloud-sdk:slim AS gsutil

RUN apt-get update \
  && apt-get install -y --no-install-recommends wget ca-certificates containerd curl \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

RUN mkdir /test

COPY --link --from=buildkit /usr/bin/buildkitd /usr/bin/buildctl /bin/
COPY --link --from=fake-gcs-server /bin/fake-gcs-server /bin/

COPY . /test
