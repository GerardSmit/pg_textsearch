################################################################
# pg_textsearch — Postgres 17 + BM25 full-text search extension
#
# Build:
#   docker build -t pg_textsearch:1.2.0 .
#
# Run (example):
#   docker run --rm -p 5432:5432 \
#     -e POSTGRES_PASSWORD=secret \
#     pg_textsearch:1.2.0
#
# Then connect from the host:
#   psql -h localhost -U postgres
#   CREATE EXTENSION pg_textsearch;
################################################################

FROM postgres:17-bookworm AS build

ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update \
 && apt-get install -y --no-install-recommends \
      build-essential \
      postgresql-server-dev-17 \
      libxml2-dev \
      pkg-config \
      ca-certificates \
      git \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /src/pg_textsearch
COPY . .

RUN make clean || true
RUN make -j"$(nproc)"
RUN make install

################################################################
# Runtime image: a slim postgres:17 with pg_textsearch installed,
# pre-configured to load via shared_preload_libraries.
################################################################
FROM postgres:17-bookworm

# Install runtime dependency for markup normalization
RUN apt-get update \
 && apt-get install -y --no-install-recommends libxml2 \
 && rm -rf /var/lib/apt/lists/*

# Copy installed artifacts from the build stage.
COPY --from=build /usr/lib/postgresql/17/lib/pg_textsearch.so \
     /usr/lib/postgresql/17/lib/pg_textsearch.so
COPY --from=build /usr/lib/postgresql/17/lib/bitcode/pg_textsearch \
     /usr/lib/postgresql/17/lib/bitcode/pg_textsearch
COPY --from=build /usr/lib/postgresql/17/lib/bitcode/pg_textsearch.index.bc \
     /usr/lib/postgresql/17/lib/bitcode/pg_textsearch.index.bc
COPY --from=build /usr/share/postgresql/17/extension/pg_textsearch* \
     /usr/share/postgresql/17/extension/

# pg_textsearch requires shared_preload_libraries. The official
# entrypoint runs scripts in /docker-entrypoint-initdb.d AFTER the
# temp server is up, which is too late to load a shared_preload
# library. Pass `-c shared_preload_libraries=pg_textsearch` to initdb
# instead — that bakes the setting into postgresql.conf BEFORE the
# temp server starts. Then 01_pg_textsearch_create.sql works against
# a server with the library already loaded.
ENV POSTGRES_INITDB_ARGS="-c shared_preload_libraries=pg_textsearch"

RUN mkdir -p /docker-entrypoint-initdb.d \
 && printf '%s\n' \
      'CREATE EXTENSION IF NOT EXISTS pg_textsearch;' \
      > /docker-entrypoint-initdb.d/00_pg_textsearch_create.sql
