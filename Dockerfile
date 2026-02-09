FROM rust:1.92-bookworm AS builder

WORKDIR /build

# Cache dependencies by building a dummy project first
COPY Cargo.toml Cargo.lock ./
RUN mkdir src && echo "fn main() {}" > src/main.rs && echo "" > src/lib.rs
RUN cargo build --release 2>/dev/null || true
RUN rm -rf src

# Build the actual project
COPY src/ src/
RUN touch src/main.rs src/lib.rs && cargo build --release --bin postgres-restore-operator

FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

RUN groupadd -r operator && useradd -r -g operator -s /sbin/nologin operator

COPY --from=builder /build/target/release/postgres-restore-operator /usr/local/bin/

USER operator

ENTRYPOINT ["postgres-restore-operator"]
