# syntax=docker/dockerfile:1.7

ARG RUST_VERSION=1.89.0
ARG DEBIAN_SUITE=bookworm

FROM --platform=$TARGETPLATFORM rust:${RUST_VERSION}-${DEBIAN_SUITE} AS build

ARG TARGETARCH

ENV CARGO_TARGET_AARCH64_UNKNOWN_LINUX_MUSL_LINKER=musl-gcc \
    CARGO_TARGET_X86_64_UNKNOWN_LINUX_MUSL_LINKER=musl-gcc \
    RUSTFLAGS="-C target-feature=+crt-static"

RUN apt-get update && apt-get install -y --no-install-recommends \
    clang \
    libclang-dev \
    musl-tools \
    pkg-config \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /workspace

COPY . .

RUN case "$TARGETARCH" in \
        amd64) rust_target=x86_64-unknown-linux-musl ;; \
        arm64) rust_target=aarch64-unknown-linux-musl ;; \
        *) echo "unsupported TARGETARCH: $TARGETARCH" >&2; exit 1 ;; \
    esac && \
    rustup target add "$rust_target" && \
    cargo build --locked --release --target "$rust_target" -p app --bin app && \
    cp "target/$rust_target/release/app" /cloudish && \
    mkdir -p /runtime-root/var/lib/cloudish/state

FROM --platform=$TARGETPLATFORM gcr.io/distroless/static-debian12:nonroot AS runtime

ENV CLOUDISH_DEFAULT_ACCOUNT=000000000000 \
    CLOUDISH_DEFAULT_REGION=eu-west-2 \
    CLOUDISH_STATE_DIR=/var/lib/cloudish/state \
    CLOUDISH_EDGE_HOST=0.0.0.0 \
    CLOUDISH_EDGE_PORT=4566 \
    CLOUDISH_EDGE_WIRE_LOG=on

COPY --from=build --chown=nonroot:nonroot /runtime-root/ /
COPY --from=build --chown=nonroot:nonroot /cloudish /cloudish

WORKDIR /var/lib/cloudish

EXPOSE 4566

ENTRYPOINT ["/cloudish"]
