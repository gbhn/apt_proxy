use apt_cacher_rs::config::Settings;
use std::collections::HashMap;
use std::path::PathBuf;
use tempfile::TempDir;

pub fn create_temp_dir() -> TempDir {
    TempDir::new().expect("Failed to create temp directory")
}

pub fn create_test_settings(cache_dir: PathBuf) -> Settings {
    Settings {
        port: 3142,
        socket: None,
        repositories: HashMap::new(),
        cache_dir,
        max_cache_size: 10 * 1024 * 1024, 
        max_lru_entries: 100,
    }
}

pub fn create_test_settings_with_repo(cache_dir: PathBuf, repo_name: &str, repo_url: &str) -> Settings {
    let mut repositories = HashMap::new();
    repositories.insert(repo_name.to_string(), repo_url.to_string());
    
    Settings {
        port: 3142,
        socket: None,
        repositories,
        cache_dir,
        max_cache_size: 10 * 1024 * 1024,
        max_lru_entries: 100,
    }
}

pub fn create_minimal_settings(cache_dir: PathBuf) -> Settings {
    Settings {
        port: 3142,
        socket: None,
        repositories: HashMap::new(),
        cache_dir,
        max_cache_size: 1024 * 1024, 
        max_lru_entries: 10,
    }
}

pub fn generate_random_data(size: usize) -> Vec<u8> {
    (0..size).map(|i| (i % 256) as u8).collect()
}

pub fn create_test_headers() -> axum::http::HeaderMap {
    let mut headers = axum::http::HeaderMap::new();
    headers.insert("content-type", "application/octet-stream".parse().unwrap());
    headers.insert("content-length", "1024".parse().unwrap());
    headers.insert("etag", "\"test-etag\"".parse().unwrap());
    headers
}

pub async fn wait_for_file(path: &std::path::Path, timeout_ms: u64) -> bool {
    let start = std::time::Instant::now();
    let timeout = std::time::Duration::from_millis(timeout_ms);
    
    while start.elapsed() < timeout {
        if path.exists() {
            return true;
        }
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
    }
    
    false
}

pub async fn read_file_with_retry(path: &std::path::Path, max_attempts: u32) -> std::io::Result<Vec<u8>> {
    for attempt in 0..max_attempts {
        match tokio::fs::read(path).await {
            Ok(data) => return Ok(data),
            Err(e) if attempt < max_attempts - 1 => {
                tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
            }
            Err(e) => return Err(e),
        }
    }
    
    Err(std::io::Error::new(
        std::io::ErrorKind::Other,
        "Max attempts exceeded",
    ))
}

pub async fn is_directory_empty(path: &std::path::Path) -> bool {
    if let Ok(mut entries) = tokio::fs::read_dir(path).await {
        entries.next_entry().await.ok().flatten().is_none()
    } else {
        true
    }
}

pub async fn calculate_directory_size(path: &std::path::Path) -> std::io::Result<u64> {
    let mut total_size = 0u64;
    let mut stack = vec![path.to_path_buf()];
    
    while let Some(current_path) = stack.pop() {
        let metadata = tokio::fs::metadata(&current_path).await?;
        
        if metadata.is_file() {
            total_size += metadata.len();
        } else if metadata.is_dir() {
            let mut entries = tokio::fs::read_dir(&current_path).await?;
            while let Some(entry) = entries.next_entry().await? {
                stack.push(entry.path());
            }
        }
    }
    
    Ok(total_size)
}

pub struct MockServerBuilder {
    server: mockito::ServerGuard,
}

impl MockServerBuilder {
    pub async fn new() -> Self {
        Self {
            server: mockito::Server::new_async().await,
        }
    }
    
    pub fn url(&self) -> String {
        self.server.url()
    }
    
    pub async fn mock_get(&mut self, path: &str, body: &[u8]) -> mockito::Mock {
        self.server
            .mock("GET", path)
            .with_status(200)
            .with_header("content-type", "application/octet-stream")
            .with_body(body)
            .create_async()
            .await
    }
    
    pub async fn mock_get_with_headers(
        &mut self,
        path: &str,
        body: &[u8],
        headers: Vec<(&str, &str)>,
    ) -> mockito::Mock {
        let mut mock = self.server.mock("GET", path).with_status(200);
        
        for (key, value) in headers {
            mock = mock.with_header(key, value);
        }
        
        mock.with_body(body).create_async().await
    }
    
    pub async fn mock_get_status(&mut self, path: &str, status: u16) -> mockito::Mock {
        self.server
            .mock("GET", path)
            .with_status(status as usize)
            .create_async()
            .await
    }
    
    pub fn into_guard(self) -> mockito::ServerGuard {
        self.server
    }
}

pub mod assertions {
    use std::path::Path;
    
    pub async fn assert_file_size(path: &Path, expected_size: u64) {
        let metadata = tokio::fs::metadata(path)
            .await
            .expect("File should exist");
        assert_eq!(
            metadata.len(),
            expected_size,
            "File size mismatch for {:?}",
            path
        );
    }
    
    pub async fn assert_file_content(path: &Path, expected_content: &[u8]) {
        let content = tokio::fs::read(path)
            .await
            .expect("File should exist and be readable");
        assert_eq!(
            content.as_slice(),
            expected_content,
            "File content mismatch for {:?}",
            path
        );
    }
    
    pub async fn assert_file_not_exists(path: &Path) {
        assert!(
            !path.exists(),
            "File should not exist: {:?}",
            path
        );
    }
    
    pub async fn assert_directory_file_count(path: &Path, expected_count: usize) {
        let mut count = 0;
        let mut entries = tokio::fs::read_dir(path)
            .await
            .expect("Directory should exist");
        
        while let Some(entry) = entries.next_entry().await.expect("Read entry") {
            if entry.metadata().await.expect("Get metadata").is_file() {
                count += 1;
            }
        }
        
        assert_eq!(
            count, expected_count,
            "Directory file count mismatch for {:?}",
            path
        );
    }
}

#[cfg(feature = "bench")]
pub mod bench {
    use std::path::PathBuf;
    
    pub fn create_bench_cache_dir() -> tempfile::TempDir {
        tempfile::TempDir::new().expect("Failed to create bench temp dir")
    }
    
    pub fn create_bench_data(size: usize) -> Vec<u8> {
        vec![0u8; size]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_generate_random_data() {
        let data = generate_random_data(100);
        assert_eq!(data.len(), 100);
    }
    
    #[test]
    fn test_create_test_headers() {
        let headers = create_test_headers();
        assert!(headers.get("content-type").is_some());
        assert!(headers.get("etag").is_some());
    }
    
    #[tokio::test]
    async fn test_wait_for_file_timeout() {
        let temp_dir = create_temp_dir();
        let path = temp_dir.path().join("nonexistent");
        
        let found = wait_for_file(&path, 100).await;
        assert!(!found);
    }
    
    #[tokio::test]
    async fn test_calculate_directory_size() {
        let temp_dir = create_temp_dir();
        
        tokio::fs::write(temp_dir.path().join("file1"), b"hello").await.unwrap();
        tokio::fs::write(temp_dir.path().join("file2"), b"world").await.unwrap();
        
        let size = calculate_directory_size(temp_dir.path()).await.unwrap();
        assert_eq!(size, 10); 
    }
    
    #[tokio::test]
    async fn test_is_directory_empty() {
        let temp_dir = create_temp_dir();
        
        assert!(is_directory_empty(temp_dir.path()).await);
        
        tokio::fs::write(temp_dir.path().join("file"), b"data").await.unwrap();
        
        assert!(!is_directory_empty(temp_dir.path()).await);
    }
}