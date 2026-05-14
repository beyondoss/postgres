FROM postgres:18

# pg_cron from the PGDG repo (already configured in the postgres:18 image).
# pgbouncer from standard Debian apt.
# postgresql-server-dev-18 + gcc to compile stub extensions.
RUN apt-get update && apt-get install -y --no-install-recommends \
    pgbouncer \
    postgresql-18-cron \
    postgresql-server-dev-18 \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Build minimal stub shared libraries for beyond_auth and beyond_queue.
# These let Postgres load both extensions via shared_preload_libraries and let
# CREATE EXTENSION succeed so post-start setup completes — no real functionality needed.
COPY stub_extension.c /tmp/stub_extension.c
RUN set -e; for ext in beyond_auth beyond_queue; do \
    gcc -I"$(pg_config --includedir-server)" -fPIC -shared \
        -o "/usr/lib/postgresql/18/lib/${ext}.so" \
        /tmp/stub_extension.c; \
    printf 'comment = '"'"'%s test stub'"'"'\ndefault_version = '"'"'0.0.1'"'"'\nmodule_pathname = '"'"'$libdir/%s'"'"'\nrelocatable = true\n' \
        "${ext}" "${ext}" > "/usr/share/postgresql/18/extension/${ext}.control"; \
    printf '' > "/usr/share/postgresql/18/extension/${ext}--0.0.1.sql"; \
    done \
    && rm /tmp/stub_extension.c

# Remove build tools to keep the image lean.
RUN apt-get remove -y --purge postgresql-server-dev-18 gcc \
    && apt-get autoremove -y \
    && rm -rf /var/lib/apt/lists/*
