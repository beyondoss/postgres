# syntax=docker/dockerfile:1
# Postgres 18 bundled with the beyond_auth extension + the queue schema, for
# local-dev / docker-compose (ghcr.io/beyondoss/beyond-postgres).
#
# This is the Docker analogue of the production rootfs: prod builds the same
# beyond_auth/beyond_queue extensions (packer/scripts/build-beyond-extensions.sh,
# pinned in extensions.toml) into an ubuntu-noble Firecracker image. Here we layer
# them onto the official postgres:18 image so a generated app can `docker pull` a
# working database. Extension sources are cloned at AUTH_REF / QUEUE_REF.
ARG AUTH_REF=main
ARG QUEUE_REF=main

# ---- build the beyond_auth pgrx extension against pg18 -----------------------
FROM postgres:18-bookworm AS extbuilder
ARG AUTH_REF
RUN apt-get update && apt-get install -y --no-install-recommends \
      curl build-essential clang libclang-dev pkg-config libssl-dev \
      postgresql-server-dev-18 ca-certificates git \
    && rm -rf /var/lib/apt/lists/*
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs \
      | sh -s -- -y --default-toolchain 1.92.0 --profile minimal
ENV PATH="/root/.cargo/bin:${PATH}"
# cargo-pgrx must match the extension's pgrx dependency (=0.18.0).
RUN cargo install cargo-pgrx --version 0.18.0 --locked
# Register pg18 (system install) so PGRX_HOME exists; no postgres is downloaded.
RUN cargo pgrx init --pg18 /usr/bin/pg_config
RUN git clone --depth 1 --branch "${AUTH_REF}" https://github.com/beyondoss/auth /src/auth
WORKDIR /src/auth/beyond-auth-extension
# Installs control + SQL + .so into this image's postgres dirs.
RUN cargo pgrx install --release --features pg18 --no-default-features --pg-config /usr/bin/pg_config

# ---- fetch the queue schema (PL/pgSQL hot paths; no pgrx ext needed) ---------
FROM alpine/git AS queuesrc
ARG QUEUE_REF
RUN git clone --depth 1 --branch "${QUEUE_REF}" https://github.com/beyondoss/queue /src/queue

# ---- final image ------------------------------------------------------------
FROM postgres:18-bookworm
COPY --from=extbuilder /usr/share/postgresql/18/extension/beyond_auth* /usr/share/postgresql/18/extension/
COPY --from=extbuilder /usr/lib/postgresql/18/lib/beyond_auth.so /usr/lib/postgresql/18/lib/
COPY --from=queuesrc /src/queue/beyond-queue-extension/sql/schema.sql /opt/queue/schema.sql
COPY --from=queuesrc /src/queue/tests/fixtures/hot_paths.sql /opt/queue/hot_paths.sql
COPY docker/initdb.sh /docker-entrypoint-initdb.d/10-beyond-init.sh
