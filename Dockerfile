FROM rust:slim-buster as build

RUN USER=root cargo new --bin berthad
WORKDIR /berthad

COPY Cargo.lock Cargo.lock
COPY Cargo.toml Cargo.toml

RUN cargo build --release
RUN rm src/*.rs

COPY src src

RUN rm ./target/release/deps/berthad*
RUN cargo build --release

FROM debian:buster-slim
MAINTAINER Lars van Rhijn
ENTRYPOINT ["/berthad-vfs", "0.0.0.0", "1234", "/berthad/data", "/berthad/tmp"]

COPY --from=build /berthad/target/release/berthad /berthad-vfs

RUN \
    mkdir --parents /berthad/data/ && \
    mkdir --parents /berthad/tmp/ && \
    \
    chmod a+x /berthad-vfs
