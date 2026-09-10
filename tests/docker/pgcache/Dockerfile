# Redis with the shared libraries the pgcache module links against.
#
# pgcache.so is built on the host (plugins/pgcache/src) and mounted in, so the
# image only has to supply libpq and jansson at runtime. The image must be
# glibc-based for the same reason: a module compiled against glibc cannot be
# relocated inside the Alpine variant.
FROM redis:7

RUN apt-get update \
    && apt-get install -y --no-install-recommends libpq5 libjansson4 \
    && rm -rf /var/lib/apt/lists/*
