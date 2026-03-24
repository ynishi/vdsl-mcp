//! テスト用共通ヘルパー。
//!
//! distribute, topology_delta 等のテストで共有するLocationId・FileFingerprint生成関数。

use super::digest::{ByteDigest, ContentDigest};
use super::fingerprint::FileFingerprint;
use super::location::LocationId;

pub fn local() -> LocationId {
    LocationId::local()
}

pub fn pod() -> LocationId {
    LocationId::new("pod").unwrap()
}

pub fn cloud() -> LocationId {
    LocationId::new("cloud").unwrap()
}

pub fn local_fp(hash: &str, size: u64) -> FileFingerprint {
    FileFingerprint {
        byte_digest: Some(ByteDigest::Djb2(hash.to_string())),
        content_digest: None,
        meta_digest: None,
        size,
        modified_at: None,
    }
}

pub fn content_fp(file_hash: &str, content_hash: &str, size: u64) -> FileFingerprint {
    FileFingerprint {
        byte_digest: Some(ByteDigest::Djb2(file_hash.to_string())),
        content_digest: Some(ContentDigest(content_hash.to_string())),
        meta_digest: None,
        size,
        modified_at: None,
    }
}

pub fn cloud_fp(size: u64) -> FileFingerprint {
    FileFingerprint {
        byte_digest: None,
        content_digest: None,
        meta_digest: None,
        size,
        modified_at: None,
    }
}
