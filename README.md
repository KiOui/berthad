# BerthaD
BerthaD is a simple, fast, no-nonsense TCP server to store blobs of data by its SHA256 hash with just three operations:

1. __PUT__ – Stores the data on disk and return its hash.
2.  __LIST__ – Return the hashes of all blobs stored on this server.
3.  __GET__ – Given the hash of a blob it returns the content of the blob.

This project is a port of the original [berthad repository](https://github.com/bertha/berthad), rewritten in 
[Rust](https://www.rust-lang.org).

## Features
* Small codebase.
* No authentication. No SSL. If you don't need them, they are only an overhead.

## Building
In order to build the project, you will first need to install [Rust](https://www.rust-lang.org). Then, to build for
development you can run:
```
cargo build
```

To build for production, you can run:
```
cargo build --release
```

## Running
Usage: `berthad <bound host> <port> <data dir> <tmp dir>`

* `bound host` is address or name of the address to be bound. For instance: `localhost` or `0.0.0.0`.
* `port` is the port on which to bind.
* `data dir` is the directory which will contain the blobs. It must exist.
* `tmp dir` is the directory which will contain the blobs while they are streamed to disk during a __PUT__.

## Protocol
The protocol is described in [PROTOCOL.md](https://github.com/KiOui/berthad/blob/master/PROTOCOL.md).
