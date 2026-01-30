use std::path::Path;
use tokio::time::{sleep, Duration};

/// Генерирует тестовые данные заданного размера
pub fn generate_test_data(size: usize) -> Vec<u8> {
    (0..size).map(|i| (i % 256) as u8).collect()
}

/// Генерирует случайные данные для более реалистичного тестирования
pub fn generate_random_data(size: usize) -> Vec<u8> {
    use rand::Rng;
    let mut rng = rand::thread_rng();
    (0..size).map(|_| rng.gen()).collect()
}

/// Создает тестовые заголовки HTTP
pub fn test_headers() -> axum::http::HeaderMap {
    let mut headers = axum::http::HeaderMap::new();
    headers.insert("content-type", "application/octet-stream".parse().unwrap());
    headers.insert("content-length", "1024".parse().unwrap());
    headers.insert("etag", "\"test-etag\"".parse().unwrap());
    headers
}

/// Создает специфичные заголовки для debian-пакетов
pub fn debian_package_headers(size: u64) -> axum::http::HeaderMap {
    let mut headers = axum::http::HeaderMap::new();
    headers.insert(
        "content-type",
        "application/x-debian-package".parse().unwrap(),
    );
    headers.insert("content-length", size.to_string().parse().unwrap());
    headers
}

/// Ожидает появления файла с таймаутом
pub async fn wait_for_file(path: &Path, timeout_ms: u64) -> bool {
    let start = std::time::Instant::now();
    let timeout = Duration::from_millis(timeout_ms);
    
    while start.elapsed() < timeout {
        if path.exists() {
            return true;
        }
        sleep(Duration::from_millis(10)).await;
    }
    
    false
}

/// Читает файл с повторными попытками (для обработки race conditions)
pub async fn read_file_with_retry(
    path: &Path,
    max_attempts: u32,
) -> std::io::Result<Vec<u8>> {
    for attempt in 0..max_attempts {
        match tokio::fs::read(path).await {
            Ok(data) => return Ok(data),
            Err(e) if attempt < max_attempts - 1 => {
                sleep(Duration::from_millis(50)).await;
            }
            Err(e) => return Err(e),
        }
    }
    
    Err(std::io::Error::new(
        std::io::ErrorKind::Other,
        "Max retry attempts exceeded",
    ))
}

/// Проверяет, пуста ли директория
pub async fn is_directory_empty(path: &Path) -> bool {
    match tokio::fs::read_dir(path).await {
        Ok(mut entries) => entries.next_entry().await.ok().flatten().is_none(),
        Err(_) => true,
    }
}

/// Подсчитывает размер директории рекурсивно
pub async fn calculate_directory_size(path: &Path) -> std::io::Result<u64> {
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

/// Подсчитывает количество файлов в директории
pub async fn count_files_in_directory(path: &Path) -> std::io::Result<usize> {
    let mut count = 0;
    let mut entries = tokio::fs::read_dir(path).await?;
    
    while let Some(entry) = entries.next_entry().await? {
        if entry.metadata().await?.is_file() {
            count += 1;
        }
    }
    
    Ok(count)
}

/// Создает вложенную структуру директорий
pub async fn create_nested_dirs(base: &Path, depth: usize) -> std::io::Result<()> {
    let mut current = base.to_path_buf();
    for i in 0..depth {
        current.push(format!("level_{}", i));
        tokio::fs::create_dir_all(&current).await?;
    }
    Ok(())
}

/// Очищает директорию, удаляя все файлы и поддиректории
pub async fn clear_directory(path: &Path) -> std::io::Result<()> {
    let mut entries = tokio::fs::read_dir(path).await?;
    
    while let Some(entry) = entries.next_entry().await? {
        let path = entry.path();
        if path.is_dir() {
            tokio::fs::remove_dir_all(&path).await?;
        } else {
            tokio::fs::remove_file(&path).await?;
        }
    }
    
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;
    
    #[test]
    fn test_generate_test_data() {
        let data = generate_test_data(100);
        assert_eq!(data.len(), 100);
        assert_eq!(data[0], 0);
        assert_eq!(data[99], 99);
    }
    
    #[test]
    fn test_generate_random_data() {
        let data1 = generate_random_data(100);
        let data2 = generate_random_data(100);
        
        assert_eq!(data1.len(), 100);
        assert_eq!(data2.len(), 100);
        // С высокой вероятностью данные должны отличаться
        assert_ne!(data1, data2);
    }
    
    #[test]
    fn test_test_headers() {
        let headers = test_headers();
        assert!(headers.get("content-type").is_some());
        assert!(headers.get("etag").is_some());
    }
    
    #[tokio::test]
    async fn test_wait_for_file_timeout() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("nonexistent");
        
        let found = wait_for_file(&path, 100).await;
        assert!(!found);
    }
    
    #[tokio::test]
    async fn test_calculate_directory_size() {
        let temp_dir = TempDir::new().unwrap();
        
        tokio::fs::write(temp_dir.path().join("file1"), b"hello")
            .await
            .unwrap();
        tokio::fs::write(temp_dir.path().join("file2"), b"world")
            .await
            .unwrap();
        
        let size = calculate_directory_size(temp_dir.path()).await.unwrap();
        assert_eq!(size, 10);
    }
    
    #[tokio::test]
    async fn test_is_directory_empty() {
        let temp_dir = TempDir::new().unwrap();
        
        assert!(is_directory_empty(temp_dir.path()).await);
        
        tokio::fs::write(temp_dir.path().join("file"), b"data")
            .await
            .unwrap();
        
        assert!(!is_directory_empty(temp_dir.path()).await);
    }
    
    #[tokio::test]
    async fn test_count_files_in_directory() {
        let temp_dir = TempDir::new().unwrap();
        
        assert_eq!(count_files_in_directory(temp_dir.path()).await.unwrap(), 0);
        
        tokio::fs::write(temp_dir.path().join("file1"), b"data")
            .await
            .unwrap();
        tokio::fs::write(temp_dir.path().join("file2"), b"data")
            .await
            .unwrap();
        
        assert_eq!(count_files_in_directory(temp_dir.path()).await.unwrap(), 2);
    }
}