use std::io;
use std::path::PathBuf;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum StorageError {
    #[error("failed to create storage directory `{path}`: {source}")]
    CreateDirectory {
        path: PathBuf,
        #[source]
        source: io::Error,
    },
    #[error("failed to read snapshot `{path}`: {source}")]
    ReadSnapshot {
        path: PathBuf,
        #[source]
        source: io::Error,
    },
    #[error("failed to decode snapshot `{path}`: {details}")]
    DecodeSnapshot { path: PathBuf, details: String },
    #[error("failed to write snapshot `{path}`: {source}")]
    WriteSnapshot {
        path: PathBuf,
        #[source]
        source: io::Error,
    },
    #[error("failed to encode snapshot `{path}`: {details}")]
    EncodeSnapshot { path: PathBuf, details: String },
    #[error("failed to replace snapshot `{path}` atomically: {source}")]
    ReplaceSnapshot {
        path: PathBuf,
        #[source]
        source: io::Error,
    },
    #[error("failed to open WAL `{path}`: {source}")]
    OpenWal {
        path: PathBuf,
        #[source]
        source: io::Error,
    },
    #[error("failed to read WAL `{path}`: {source}")]
    ReadWal {
        path: PathBuf,
        #[source]
        source: io::Error,
    },
    #[error("failed to write WAL `{path}`: {source}")]
    WriteWal {
        path: PathBuf,
        #[source]
        source: io::Error,
    },
    #[error("failed to encode WAL entry for `{path}`: {details}")]
    EncodeWal { path: PathBuf, details: String },
    #[error("failed to decode WAL entry from `{path}`: {details}")]
    DecodeWal { path: PathBuf, details: String },
    #[error("encountered unknown WAL op-code {op:#04x} in `{path}`")]
    UnknownWalOp { path: PathBuf, op: u8 },
}
