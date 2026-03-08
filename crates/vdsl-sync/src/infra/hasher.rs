//! Content identity hashing.
//!
//! Two-layer hash model:
//! - **`file_hash`** — DJB2 of entire file bytes. Required for all files.
//!   Used for change detection and generic duplicate detection.
//! - **`content_hash`** — format-specific semantic hash (e.g. DJB2 of PNG IHDR+IDAT).
//!   Used for high-precision duplicate detection that ignores metadata differences.
//!
//! [`ContentHasher`] is pluggable — default implementation ([`Djb2Hasher`]) computes
//! both layers. PNG content_hash is Lua-compatible (`png.image_hash`).

use std::io::{BufReader, Read, Seek, SeekFrom};
use std::path::Path;

use crate::domain::error::SyncError;

/// Result of hashing a file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HashResult {
    /// DJB2 hash of entire file content. Always present.
    pub file_hash: String,
    /// Format-specific semantic hash (e.g. PNG pixel identity).
    /// Present only for supported formats.
    pub content_hash: Option<String>,
}

/// Pluggable content identity resolver.
///
/// Computes both generic file hash and format-specific content hash.
pub trait ContentHasher: Send + Sync {
    /// Compute hashes for the given file.
    ///
    /// `file_hash` is always computed. `content_hash` is computed
    /// only for supported formats (e.g. PNG).
    fn hash_file(&self, path: &Path) -> Result<HashResult, SyncError>;
}

/// Default hasher: DJB2 for all files + PNG IHDR+IDAT semantic hash.
///
/// - `file_hash`: DJB2 of entire file bytes (`%016x`).
/// - `content_hash`: For PNG files, DJB2 of IHDR+IDAT chunks (`%016x`).
///   Produces the same hash as Lua's `png.image_hash()`.
pub struct Djb2Hasher;

impl ContentHasher for Djb2Hasher {
    fn hash_file(&self, path: &Path) -> Result<HashResult, SyncError> {
        let file_hash = djb2_file_hash(path)?;

        // content_hash is format-specific. Currently only PNG is supported.
        // When adding new formats, update both here and FileType::from_extension.
        let ext = path.extension().and_then(|e| e.to_str()).unwrap_or("");
        let content_hash = if ext.eq_ignore_ascii_case("png") {
            Some(png_image_hash(path)?)
        } else {
            None
        };

        Ok(HashResult {
            file_hash,
            content_hash,
        })
    }
}

/// Compute DJB2 hash of entire file content.
///
/// Reads file in 8KB chunks for memory efficiency.
/// Returns 16-char hex string (`%016x`).
pub fn djb2_file_hash(path: &Path) -> Result<String, SyncError> {
    let file =
        std::fs::File::open(path).map_err(|e| SyncError::Hash(format!("open failed: {e}")))?;
    let mut reader = BufReader::new(file);

    let mut h: u64 = 5381;
    let mut buf = [0u8; 8192];
    loop {
        let n = reader
            .read(&mut buf)
            .map_err(|e| SyncError::Hash(format!("read failed: {e}")))?;
        if n == 0 {
            break;
        }
        // Use full u64 width for file hash (unlike PNG hash which uses 32-bit for Lua compat).
        // This reduces collision probability from ~1/2^32 to ~1/2^64.
        for &b in &buf[..n] {
            h = h.wrapping_mul(33).wrapping_add(b as u64);
        }
    }
    Ok(format!("{h:016x}"))
}

/// Compute DJB2 hash of IHDR + IDAT chunk data in a PNG file.
///
/// Algorithm matches Lua's `png.image_hash(filepath)`:
/// 1. Verify PNG signature
/// 2. Walk chunks, for IHDR and IDAT: feed chunk_type + chunk_data into DJB2
/// 3. Return 16-char hex string (`%016x`)
pub fn png_image_hash(path: &Path) -> Result<String, SyncError> {
    let file =
        std::fs::File::open(path).map_err(|e| SyncError::Hash(format!("open failed: {e}")))?;
    let mut reader = BufReader::new(file);

    // Verify PNG signature
    let mut sig = [0u8; 8];
    reader
        .read_exact(&mut sig)
        .map_err(|e| SyncError::Hash(format!("read sig failed: {e}")))?;
    if sig != [137, 80, 78, 71, 13, 10, 26, 10] {
        return Err(SyncError::Hash("not a valid PNG file".into()));
    }

    let mut h: u64 = 5381;
    let mut reached_iend = false;

    loop {
        let mut header = [0u8; 8];
        match reader.read_exact(&mut header) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
            Err(e) => return Err(SyncError::Hash(format!("read chunk header failed: {e}"))),
        }
        // u32 → u64: always lossless (max 4_294_967_295). Further bounded by MAX_CHUNK_LEN below.
        let length = u32::from_be_bytes([header[0], header[1], header[2], header[3]]) as u64;
        // Guard against malicious chunk lengths (PNG spec max is 2^31-1)
        const MAX_CHUNK_LEN: u64 = 0x7FFF_FFFF;
        if length > MAX_CHUNK_LEN {
            return Err(SyncError::Hash(format!(
                "chunk length exceeds PNG spec maximum: {length}"
            )));
        }
        let chunk_type = &header[4..8];

        if chunk_type == b"IEND" {
            reached_iend = true;
            break;
        }

        if chunk_type == b"IHDR" || chunk_type == b"IDAT" {
            // Hash chunk_type bytes
            // Lua-compatible: compute DJB2 in 32-bit width (% 0x100000000)
            // to produce the same value as Lua's png.image_hash().
            for &b in chunk_type {
                h = h.wrapping_mul(33).wrapping_add(b as u64) % 0x100000000;
            }
            // Hash chunk data
            let mut remaining = length;
            let mut buf = [0u8; 8192];
            while remaining > 0 {
                let to_read = std::cmp::min(remaining, buf.len() as u64) as usize;
                reader
                    .read_exact(&mut buf[..to_read])
                    .map_err(|e| SyncError::Hash(format!("read data failed: {e}")))?;
                for &b in &buf[..to_read] {
                    h = h.wrapping_mul(33).wrapping_add(b as u64) % 0x100000000;
                }
                remaining -= to_read as u64;
            }
            // Skip CRC (4 bytes)
            reader
                .seek(SeekFrom::Current(4))
                .map_err(|e| SyncError::Hash(format!("seek crc failed: {e}")))?;
        } else {
            // Skip chunk data + CRC
            let skip = i64::try_from(length)
                .map_err(|_| SyncError::Hash(format!("chunk length overflow: {length}")))?
                + 4;
            reader
                .seek(SeekFrom::Current(skip))
                .map_err(|e| SyncError::Hash(format!("seek skip failed: {e}")))?;
        }
    }

    if !reached_iend {
        return Err(SyncError::Hash(
            "truncated PNG: IEND chunk not found".into(),
        ));
    }

    Ok(format!("{h:016x}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Build a minimal valid PNG with given IDAT data and optional tEXt chunks.
    fn build_test_png(idat_data: &[u8], text_chunks: &[(&str, &str)]) -> Vec<u8> {
        let mut buf = Vec::new();
        // PNG signature
        buf.extend_from_slice(&[137, 80, 78, 71, 13, 10, 26, 10]);

        // IHDR (1x1 RGB)
        let ihdr = [0, 0, 0, 1, 0, 0, 0, 1, 8, 2, 0, 0, 0];
        buf.extend_from_slice(&(ihdr.len() as u32).to_be_bytes());
        buf.extend_from_slice(b"IHDR");
        buf.extend_from_slice(&ihdr);
        buf.extend_from_slice(&[0, 0, 0, 0]); // CRC placeholder

        // tEXt chunks
        for (keyword, text) in text_chunks {
            let data: Vec<u8> = [keyword.as_bytes(), &[0], text.as_bytes()].concat();
            buf.extend_from_slice(&(data.len() as u32).to_be_bytes());
            buf.extend_from_slice(b"tEXt");
            buf.extend_from_slice(&data);
            buf.extend_from_slice(&[0, 0, 0, 0]); // CRC placeholder
        }

        // IDAT
        buf.extend_from_slice(&(idat_data.len() as u32).to_be_bytes());
        buf.extend_from_slice(b"IDAT");
        buf.extend_from_slice(idat_data);
        buf.extend_from_slice(&[0, 0, 0, 0]); // CRC placeholder

        // IEND
        buf.extend_from_slice(&0u32.to_be_bytes());
        buf.extend_from_slice(b"IEND");
        buf.extend_from_slice(&[0, 0, 0, 0]); // CRC placeholder

        buf
    }

    // =========================================================================
    // djb2_file_hash — generic file hash
    // =========================================================================

    #[test]
    fn file_hash_non_empty() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("data.json");
        std::fs::write(&path, b"{}").unwrap();
        let hash = djb2_file_hash(&path).unwrap();
        assert_eq!(hash.len(), 16);
        assert!(hash.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn file_hash_deterministic() {
        let dir = tempfile::tempdir().unwrap();
        let p1 = dir.path().join("a.txt");
        let p2 = dir.path().join("b.txt");
        std::fs::write(&p1, b"hello world").unwrap();
        std::fs::write(&p2, b"hello world").unwrap();
        assert_eq!(djb2_file_hash(&p1).unwrap(), djb2_file_hash(&p2).unwrap());
    }

    #[test]
    fn file_hash_different_content() {
        let dir = tempfile::tempdir().unwrap();
        let p1 = dir.path().join("a.txt");
        let p2 = dir.path().join("b.txt");
        std::fs::write(&p1, b"content_a").unwrap();
        std::fs::write(&p2, b"content_b").unwrap();
        assert_ne!(djb2_file_hash(&p1).unwrap(), djb2_file_hash(&p2).unwrap());
    }

    #[test]
    fn file_hash_empty_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("empty");
        std::fs::write(&path, b"").unwrap();
        let hash = djb2_file_hash(&path).unwrap();
        // DJB2 initial value 5381 = 0x1505
        assert_eq!(hash, "0000000000001505");
    }

    // =========================================================================
    // png_image_hash — PNG semantic hash
    // =========================================================================

    #[test]
    fn png_hash_valid() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.png");
        std::fs::write(&path, build_test_png(b"PIXEL_DATA", &[])).unwrap();

        let hash = png_image_hash(&path).unwrap();
        assert_eq!(hash.len(), 16);
        assert!(hash.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn png_hash_not_png() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("not.png");
        std::fs::write(&path, b"not a png").unwrap();
        assert!(png_image_hash(&path).is_err());
    }

    #[test]
    fn png_same_pixels_different_metadata() {
        let dir = tempfile::tempdir().unwrap();
        let p1 = dir.path().join("a.png");
        let p2 = dir.path().join("b.png");
        std::fs::write(&p1, build_test_png(b"SAME_PIXELS", &[])).unwrap();
        std::fs::write(
            &p2,
            build_test_png(b"SAME_PIXELS", &[("vdsl", r#"{"seed":42}"#)]),
        )
        .unwrap();

        let h1 = png_image_hash(&p1).unwrap();
        let h2 = png_image_hash(&p2).unwrap();
        assert_eq!(h1, h2, "same pixels must yield same content_hash");
    }

    #[test]
    fn png_different_pixels() {
        let dir = tempfile::tempdir().unwrap();
        let p1 = dir.path().join("a.png");
        let p2 = dir.path().join("b.png");
        std::fs::write(&p1, build_test_png(b"PIXELS_AAA", &[])).unwrap();
        std::fs::write(&p2, build_test_png(b"PIXELS_BBB", &[])).unwrap();

        assert_ne!(png_image_hash(&p1).unwrap(), png_image_hash(&p2).unwrap());
    }

    #[test]
    fn png_deterministic() {
        let dir = tempfile::tempdir().unwrap();
        let p1 = dir.path().join("d1.png");
        let p2 = dir.path().join("d2.png");
        let data = build_test_png(b"DETERMINISTIC", &[]);
        std::fs::write(&p1, &data).unwrap();
        std::fs::write(&p2, &data).unwrap();

        let h1 = png_image_hash(&p1).unwrap();
        let h2 = png_image_hash(&p2).unwrap();
        assert_eq!(h1, h2);
        assert_ne!(h1, "0000000000001505");
    }

    // =========================================================================
    // Djb2Hasher (ContentHasher trait)
    // =========================================================================

    #[test]
    fn hasher_non_png_no_content_hash() {
        let hasher = Djb2Hasher;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("data.json");
        std::fs::write(&path, b"{}").unwrap();
        let result = hasher.hash_file(&path).unwrap();
        assert_eq!(result.file_hash.len(), 16);
        assert!(result.content_hash.is_none());
    }

    #[test]
    fn hasher_png_has_both_hashes() {
        let hasher = Djb2Hasher;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.png");
        std::fs::write(&path, build_test_png(b"DATA", &[])).unwrap();
        let result = hasher.hash_file(&path).unwrap();
        assert_eq!(result.file_hash.len(), 16);
        assert!(result.content_hash.is_some());
        assert_eq!(result.content_hash.as_ref().unwrap().len(), 16);
    }

    #[test]
    fn hasher_png_file_hash_differs_from_content_hash() {
        let hasher = Djb2Hasher;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.png");
        std::fs::write(&path, build_test_png(b"PIXEL_DATA", &[])).unwrap();
        let result = hasher.hash_file(&path).unwrap();
        // file_hash includes PNG signature, tEXt, CRC etc — content_hash only IHDR+IDAT
        assert_ne!(
            result.file_hash,
            result.content_hash.unwrap(),
            "file_hash (whole file) and content_hash (IHDR+IDAT) should differ"
        );
    }

    #[test]
    fn hasher_png_same_pixels_different_metadata_same_content_different_file() {
        let hasher = Djb2Hasher;
        let dir = tempfile::tempdir().unwrap();
        let p1 = dir.path().join("a.png");
        let p2 = dir.path().join("b.png");
        std::fs::write(&p1, build_test_png(b"SAME", &[])).unwrap();
        std::fs::write(&p2, build_test_png(b"SAME", &[("key", "metadata")])).unwrap();
        let r1 = hasher.hash_file(&p1).unwrap();
        let r2 = hasher.hash_file(&p2).unwrap();

        // content_hash is same (same pixels)
        assert_eq!(r1.content_hash, r2.content_hash);
        // file_hash differs (different metadata chunks → different total bytes)
        assert_ne!(r1.file_hash, r2.file_hash);
    }

    /// Cross-language hash verification.
    /// Requires Lua test to have written /tmp/vdsl_hash_test.png and .lua_hash.
    /// Run explicitly: `cargo test cross_language_hash_match -- --ignored`
    #[test]
    #[ignore]
    fn cross_language_hash_match() {
        let png_path = Path::new("/tmp/vdsl_hash_test.png");
        let hash_path = Path::new("/tmp/vdsl_hash_test.lua_hash");
        assert!(
            png_path.exists() && hash_path.exists(),
            "required fixture files not found: /tmp/vdsl_hash_test.png and .lua_hash"
        );
        let rust_hash = png_image_hash(png_path).unwrap();
        let lua_hash = std::fs::read_to_string(hash_path)
            .unwrap()
            .trim()
            .to_string();
        assert_eq!(
            rust_hash, lua_hash,
            "Rust hash ({rust_hash}) must match Lua hash ({lua_hash})"
        );
    }
}
