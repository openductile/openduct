# syntax=docker/dockerfile:1
# OpenDuct's container image: the broker and, beside it, the studio - built from this repository, run as an
# unprivileged user with its data in /data.
#
#   docker build -t openduct:local .
#   docker run -p 5480:5480 -p 9092:9092 -e OPENDUCT_ADVERTISE=localhost:9092 -e OPENDUCT_ALLOW_INSECURE=true \
#     -v openduct-data:/data openduct:local      # a laptop only; then open http://localhost:5480
#
# Listening on 0.0.0.0, the broker refuses to start without TLS, sign-in and access rules (docs/security.md) unless
# OPENDUCT_ALLOW_INSECURE=true says otherwise. With TLS on, the health check below needs OPENDUCT_CLIENT_TLS_CA and
# OPENDUCT_CLIENT_TLS_SERVER_NAME (the name in the broker's certificate). `serve` instead of `start` runs the broker
# alone.
#
# Every `openduct start` setting can be given as an OPENDUCT_* environment variable (`openduct start --help`).

# The studio's page.
FROM node:22.22.3-bookworm-slim AS page
WORKDIR /ui
COPY ui/package.json ui/package-lock.json ./
RUN npm ci --no-audit --no-fund
COPY ui/ ./
RUN npm run build

FROM rust:1.100.0-slim-bookworm AS build
# How many compile jobs at once (a laptop's Docker shares its machine).
ARG JOBS=4
WORKDIR /src
COPY . .
# Built into the program.
COPY --from=page /ui/dist ui/dist
# The registry and build caches stay between builds; the binary is copied out of the cache.
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    --mount=type=cache,target=/src/target \
    cargo build --release --locked -j "$JOBS" -p openduct \
    && cargo build --release --locked -j "$JOBS" -p openduct-script --bin openduct-python \
    && cp target/release/openduct target/release/openduct-python /

FROM debian:bookworm-slim
# Root certificates, for steps and connectors that verify a server's TLS certificate.
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --uid 10001 --home-dir /data openduct \
    && mkdir /data && chown openduct /data
COPY --from=build /openduct /openduct-python /usr/local/bin/
# What Python steps' program is made of, and the licences that come with it.
COPY licenses/openduct-python/ /usr/share/doc/openduct-python/
USER openduct
ENV OPENDUCT_LISTEN=0.0.0.0:9092 \
    OPENDUCT_STUDIO_LISTEN=0.0.0.0:5480 \
    OPENDUCT_DATA_DIR=/data
VOLUME /data
# 9092: Kafka clients; 9093: the other brokers of a cluster; 5480: the studio, and webhooks.
EXPOSE 9092 9093 5480
HEALTHCHECK --interval=10s --timeout=3s CMD ["openduct", "health", "--bootstrap", "127.0.0.1:9092"]
ENTRYPOINT ["openduct"]
CMD ["start"]
