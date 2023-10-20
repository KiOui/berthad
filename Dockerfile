FROM rust:slim-buster
MAINTAINER Lars van Rhijn

ENTRYPOINT ["/berthad-vfs", "0.0.0.0", "1234", "/berthad/data", "/berthad/tmp"]

WORKDIR /berthad

COPY src /berthad/build/src
COPY Cargo.toml /berthad/build/
COPY Cargo.lock /berthad/build/

RUN \
    mkdir --parents /berthad/data/ && \
    mkdir --parents /berthad/tmp/ && \
    \
    cd build && \
    cargo build --release && \
    mv /berthad/build/target/release/berthad /berthad-vfs && \
    chmod a+x /berthad-vfs