################################################################
# pg_textsearch — Postgres 18 + BM25 full-text search + pgvector
#
# Build:
#   docker build -t pg_textsearch .
#
# Run (example):
#   docker run --rm -p 5432:5432 \
#     -e POSTGRES_PASSWORD=secret \
#     pg_textsearch
#
# Then connect from the host:
#   psql -h localhost -U postgres
################################################################

FROM postgres:18-bookworm AS build

ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update \
 && apt-get install -y --no-install-recommends \
      build-essential \
      postgresql-server-dev-18 \
      libxml2-dev \
      pkg-config \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /src/pg_textsearch
COPY . .

RUN make clean || true \
 && make -j"$(nproc)" \
 && make install

################################################################
# Runtime image: postgres:18 with pg_textsearch, pgvector, and
# pg_trgm pre-configured via shared_preload_libraries.
################################################################
FROM postgres:18-bookworm

RUN apt-get update \
 && apt-get install -y --no-install-recommends \
      libxml2 \
      postgresql-18-pgvector \
 && rm -rf /var/lib/apt/lists/*

COPY --from=build /usr/lib/postgresql/18/lib/pg_textsearch.so \
     /usr/lib/postgresql/18/lib/pg_textsearch.so
COPY --from=build /usr/lib/postgresql/18/lib/bitcode/pg_textsearch \
     /usr/lib/postgresql/18/lib/bitcode/pg_textsearch
COPY --from=build /usr/lib/postgresql/18/lib/bitcode/pg_textsearch.index.bc \
     /usr/lib/postgresql/18/lib/bitcode/pg_textsearch.index.bc
COPY --from=build /usr/share/postgresql/18/extension/pg_textsearch* \
     /usr/share/postgresql/18/extension/

ENV POSTGRES_INITDB_ARGS="-c shared_preload_libraries=pg_textsearch"

RUN mkdir -p /docker-entrypoint-initdb.d \
 && printf '%s\n' \
      'CREATE EXTENSION IF NOT EXISTS pg_textsearch;' \
      'CREATE EXTENSION IF NOT EXISTS vector;' \
      'CREATE EXTENSION IF NOT EXISTS pg_trgm;' \
      > /docker-entrypoint-initdb.d/00_extensions.sql
