# Static linux/amd64 binary in an empty image.
# Builder: glibc with crt-static (no target-cpu=native; SIMD is runtime-dispatched by memchr).
FROM rust:1-slim-bookworm AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN RUSTFLAGS="-C target-feature=+crt-static" \
    cargo build --release --locked --target x86_64-unknown-linux-gnu \
 && cp target/x86_64-unknown-linux-gnu/release/shopping-sessions /shopping-sessions

FROM scratch
COPY --from=build /shopping-sessions /shopping-sessions
USER 1000:1000
CMD ["/shopping-sessions"]
