//! Thumbnail cache management for the MCP server.
//!
//! This module provides functionality to cache thumbnail images on disk
//! and serve them via HTTP URLs, avoiding the need to transmit large
//! base64-encoded images in MCP responses.

use chrono::Utc;
use serde::{Deserialize, Serialize};
use std::fs::{self, File};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::time::Duration;
use tracing::{debug, info, warn};

/// Default TTL for cached thumbnails
pub const DEFAULT_TTL: Duration = Duration::from_secs(24 * 60 * 60); // 24 hours

/// File extension for cached thumbnails
const THUMBNAIL_EXTENSION: &str = "png";

/// Metadata file extension
const METADATA_EXTENSION: &str = "meta";

/// Thumbnail cache configuration
#[derive(Clone, Debug)]
pub struct ThumbnailCacheConfig {
    /// Directory to store cached thumbnails
    pub cache_dir: PathBuf,
    /// Time-to-live for cached thumbnails
    pub ttl: Duration,
    /// Base URL for serving thumbnails
    pub base_url: String,
}

impl ThumbnailCacheConfig {
    pub fn new(cache_dir: PathBuf, ttl: Duration, host: &str, port: u16) -> Self {
        let base_url = format!("http://{}:{}/thumbnail", host, port);
        Self {
            cache_dir,
            ttl,
            base_url,
        }
    }
}

/// Metadata stored alongside each cached thumbnail
#[derive(Debug, Serialize, Deserialize)]
pub struct ThumbnailMetadata {
    /// When the thumbnail was cached (as Unix timestamp in milliseconds)
    pub cached_at: i64,
    /// Original asset path or UUID (for reference)
    pub source: String,
    /// Content hash (optional, for deduplication)
    pub content_hash: Option<String>,
}

/// Thumbnail cache for storing and retrieving cached thumbnails
pub struct ThumbnailCache {
    config: ThumbnailCacheConfig,
}

impl ThumbnailCache {
    /// Create a new thumbnail cache with the given configuration
    pub fn new(config: ThumbnailCacheConfig) -> Result<Self, String> {
        let cache = Self { config };
        cache.ensure_cache_dir()?;
        Ok(cache)
    }

    /// Get the cache directory path
    pub fn cache_dir(&self) -> &Path {
        &self.config.cache_dir
    }

    /// Get the base URL for serving thumbnails
    pub fn base_url(&self) -> &str {
        &self.config.base_url
    }

    /// Get the TTL for cached thumbnails
    pub fn ttl(&self) -> Duration {
        self.config.ttl
    }

    /// Ensure the cache directory exists
    fn ensure_cache_dir(&self) -> Result<(), String> {
        fs::create_dir_all(&self.config.cache_dir).map_err(|err| {
            format!(
                "Failed to create thumbnail cache directory {:?}: {}",
                self.config.cache_dir, err
            )
        })?;
        debug!(
            "Thumbnail cache directory ready: {:?}",
            self.config.cache_dir
        );
        Ok(())
    }

    /// Generate a unique cache key for a thumbnail
    ///
    /// The cache key is based on the source identifier (path or UUID) and a timestamp
    /// to allow multiple versions of the same asset to be cached.
    pub fn generate_cache_key(&self, source: &str) -> String {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};
        use std::time::{SystemTime, UNIX_EPOCH};

        let mut hasher = DefaultHasher::new();
        source.hash(&mut hasher);
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or(Duration::ZERO)
            .as_millis();
        timestamp.hash(&mut hasher);

        format!("{:016x}", hasher.finish())
    }

    /// Save a thumbnail to the cache
    ///
    /// Returns the cache key and the full URL for accessing the thumbnail
    pub fn save_thumbnail(&self, source: &str, data: &[u8]) -> Result<(String, String), String> {
        let cache_key = self.generate_cache_key(source);
        let file_path = self.cache_path(&cache_key);
        let meta_path = self.metadata_path(&cache_key);

        // Write the thumbnail data
        let mut file = File::create(&file_path)
            .map_err(|err| format!("Failed to create thumbnail file {:?}: {}", file_path, err))?;
        file.write_all(data)
            .map_err(|err| format!("Failed to write thumbnail data to {:?}: {}", file_path, err))?;

        // Write the metadata
        let metadata = ThumbnailMetadata {
            cached_at: Utc::now().timestamp_millis(),
            source: source.to_string(),
            content_hash: None,
        };
        let meta_json = serde_json::to_string_pretty(&metadata)
            .map_err(|err| format!("Failed to serialize thumbnail metadata: {}", err))?;
        fs::write(&meta_path, meta_json)
            .map_err(|err| format!("Failed to write metadata file {:?}: {}", meta_path, err))?;

        let url = format!("{}/{}", self.config.base_url, cache_key);
        info!(
            "Cached thumbnail for '{}' at {} (expires in {:?})",
            source, url, self.config.ttl
        );

        Ok((cache_key, url))
    }

    /// Load a thumbnail from the cache
    ///
    /// Returns the thumbnail data if found and not expired
    pub fn load_thumbnail(&self, cache_key: &str) -> Result<Vec<u8>, String> {
        let file_path = self.cache_path(cache_key);

        // Check if file exists
        if !file_path.exists() {
            return Err(format!("Thumbnail not found: {}", cache_key));
        }

        // Check if expired
        if self.is_expired(cache_key) {
            // Clean up expired thumbnail
            let _ = self.remove_thumbnail(cache_key);
            return Err(format!("Thumbnail expired and removed: {}", cache_key));
        }

        // Read the thumbnail data
        let mut file = File::open(&file_path)
            .map_err(|err| format!("Failed to open thumbnail file {:?}: {}", file_path, err))?;
        let mut data = Vec::new();
        file.read_to_end(&mut data).map_err(|err| {
            format!(
                "Failed to read thumbnail data from {:?}: {}",
                file_path, err
            )
        })?;

        debug!("Loaded thumbnail from cache: {}", cache_key);
        Ok(data)
    }

    /// Check if a thumbnail is expired
    fn is_expired(&self, cache_key: &str) -> bool {
        let meta_path = self.metadata_path(cache_key);
        if !meta_path.exists() {
            return true;
        }

        // Read metadata and check age
        match fs::read_to_string(&meta_path) {
            Ok(content) => {
                if let Ok(metadata) = serde_json::from_str::<ThumbnailMetadata>(&content) {
                    let now = Utc::now().timestamp_millis();
                    let age_millis = now - metadata.cached_at;
                    let ttl_millis = self.config.ttl.as_millis() as i64;
                    age_millis > ttl_millis
                } else {
                    true // Invalid metadata means expired
                }
            }
            Err(_) => true,
        }
    }

    /// Remove a thumbnail and its metadata from the cache
    pub fn remove_thumbnail(&self, cache_key: &str) -> Result<(), String> {
        let file_path = self.cache_path(cache_key);
        let meta_path = self.metadata_path(cache_key);

        if file_path.exists() {
            fs::remove_file(&file_path).map_err(|err| {
                format!("Failed to remove thumbnail file {:?}: {}", file_path, err)
            })?;
        }
        if meta_path.exists() {
            fs::remove_file(&meta_path).map_err(|err| {
                format!("Failed to remove metadata file {:?}: {}", meta_path, err)
            })?;
        }

        debug!("Removed thumbnail from cache: {}", cache_key);
        Ok(())
    }

    /// Clean up all expired thumbnails
    ///
    /// Returns the number of thumbnails removed
    pub fn cleanup_expired(&self) -> Result<usize, String> {
        let mut removed = 0;

        let entries = fs::read_dir(&self.config.cache_dir).map_err(|err| {
            format!(
                "Failed to read cache directory {:?}: {}",
                self.config.cache_dir, err
            )
        })?;

        for entry in entries.flatten() {
            let path = entry.path();
            if path.extension().and_then(|s| s.to_str()) != Some(THUMBNAIL_EXTENSION) {
                continue;
            }

            let cache_key = match path.file_stem().and_then(|s| s.to_str()) {
                Some(key) => key,
                None => continue,
            };

            if self.is_expired(cache_key) {
                if let Err(err) = self.remove_thumbnail(cache_key) {
                    warn!("Failed to remove expired thumbnail {}: {}", cache_key, err);
                } else {
                    removed += 1;
                }
            }
        }

        if removed > 0 {
            info!("Cleaned up {} expired thumbnail(s)", removed);
        }
        Ok(removed)
    }

    /// Get the file path for a cached thumbnail
    fn cache_path(&self, cache_key: &str) -> PathBuf {
        let mut path = self.config.cache_dir.clone();
        path.push(format!("{}.{}", cache_key, THUMBNAIL_EXTENSION));
        path
    }

    /// Get the metadata file path for a cached thumbnail
    fn metadata_path(&self, cache_key: &str) -> PathBuf {
        let mut path = self.config.cache_dir.clone();
        path.push(format!("{}.{}", cache_key, METADATA_EXTENSION));
        path
    }
}

/// Get the default cache directory path
///
/// Uses ~/.pcli2-mcp/thumbnails on Unix-like systems
pub fn default_cache_dir() -> Result<PathBuf, String> {
    let home_dir = std::env::var("HOME")
        .map(PathBuf::from)
        .or_else(|_| std::env::var("USERPROFILE").map(PathBuf::from))
        .map_err(|_| "Could not determine home directory".to_string())?;

    let mut cache_dir = home_dir;
    cache_dir.push(".pcli2-mcp");
    cache_dir.push("thumbnails");
    Ok(cache_dir)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::env;

    fn create_test_cache() -> (ThumbnailCache, PathBuf) {
        let mut temp_dir = env::temp_dir();
        temp_dir.push(format!("pcli2-thumbnail-test-{}", std::process::id()));

        let config = ThumbnailCacheConfig {
            cache_dir: temp_dir.clone(),
            ttl: DEFAULT_TTL,
            base_url: "http://localhost:8080/thumbnail".to_string(),
        };

        let cache = ThumbnailCache::new(config).unwrap();
        (cache, temp_dir)
    }

    #[test]
    fn test_generate_cache_key() {
        let (cache, _temp_dir) = create_test_cache();
        let key1 = cache.generate_cache_key("test-source-1");
        let key2 = cache.generate_cache_key("test-source-2");

        // Keys should be different for different sources
        assert_ne!(key1, key2);

        // Keys should be hex strings of consistent length
        assert_eq!(key1.len(), 16);
        assert!(key1.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn test_save_and_load_thumbnail() {
        let (cache, _temp_dir) = create_test_cache();
        let source = "test-asset-123";
        let data = b"fake png data";

        let (cache_key, url) = cache.save_thumbnail(source, data).unwrap();

        // Check that URL is correct
        assert!(url.starts_with("http://localhost:8080/thumbnail/"));
        assert!(url.ends_with(&cache_key));

        // Check that we can load the thumbnail back
        let loaded = cache.load_thumbnail(&cache_key).unwrap();
        assert_eq!(loaded, data);
    }

    #[test]
    fn test_load_nonexistent_thumbnail() {
        let (cache, _temp_dir) = create_test_cache();
        let result = cache.load_thumbnail("nonexistent-key");
        assert!(result.is_err());
    }

    #[test]
    fn test_remove_thumbnail() {
        let (cache, _temp_dir) = create_test_cache();
        let source = "test-asset-456";
        let data = b"fake png data to remove";

        let (cache_key, _) = cache.save_thumbnail(source, data).unwrap();

        // Verify it exists
        assert!(cache.load_thumbnail(&cache_key).is_ok());

        // Remove it
        cache.remove_thumbnail(&cache_key).unwrap();

        // Verify it's gone
        assert!(cache.load_thumbnail(&cache_key).is_err());
    }

    #[test]
    fn test_cleanup_expired() {
        let mut temp_dir = env::temp_dir();
        temp_dir.push(format!(
            "pcli2-thumbnail-expiry-test-{}",
            std::process::id()
        ));

        // Create cache with very short TTL for testing
        let config = ThumbnailCacheConfig {
            cache_dir: temp_dir.clone(),
            ttl: Duration::from_millis(100), // 100ms TTL
            base_url: "http://localhost:8080/thumbnail".to_string(),
        };

        let cache = ThumbnailCache::new(config).unwrap();
        let source = "test-asset-expiry";
        let data = b"fake png data for expiry test";

        let (cache_key, _) = cache.save_thumbnail(source, data).unwrap();

        // Should exist immediately
        assert!(cache.load_thumbnail(&cache_key).is_ok());

        // Wait for expiry
        std::thread::sleep(Duration::from_millis(150));

        // Should be expired now
        let result = cache.load_thumbnail(&cache_key);
        assert!(result.is_err());

        // Cleanup should report 0 since load already removed it
        let cleaned = cache.cleanup_expired().unwrap();
        assert_eq!(cleaned, 0);
    }

    #[test]
    fn test_default_cache_dir() {
        let result = default_cache_dir();
        assert!(result.is_ok());

        let cache_dir = result.unwrap();
        assert!(cache_dir.ends_with("thumbnails"));
        assert!(cache_dir.to_string_lossy().contains(".pcli2-mcp"));
    }
}
