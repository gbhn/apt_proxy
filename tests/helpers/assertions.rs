use std::path::Path;

/// Проверяет размер файла
pub async fn assert_file_size(path: &Path, expected_size: u64) {
    let metadata = tokio::fs::metadata(path)
        .await
        .unwrap_or_else(|_| panic!("File should exist: {:?}", path));
    
    assert_eq!(
        metadata.len(),
        expected_size,
        "File size mismatch for {:?}. Expected: {}, Got: {}",
        path,
        expected_size,
        metadata.len()
    );
}

/// Проверяет содержимое файла
pub async fn assert_file_content(path: &Path, expected_content: &[u8]) {
    let content = tokio::fs::read(path)
        .await
        .unwrap_or_else(|_| panic!("File should exist and be readable: {:?}", path));
    
    assert_eq!(
        content.as_slice(),
        expected_content,
        "File content mismatch for {:?}",
        path
    );
}

/// Проверяет, что файл не существует
pub async fn assert_file_not_exists(path: &Path) {
    assert!(
        !path.exists(),
        "File should not exist: {:?}",
        path
    );
}

/// Проверяет, что файл существует
pub async fn assert_file_exists(path: &Path) {
    assert!(
        path.exists(),
        "File should exist: {:?}",
        path
    );
}

/// Проверяет количество файлов в директории
pub async fn assert_directory_file_count(path: &Path, expected_count: usize) {
    let mut count = 0;
    let mut entries = tokio::fs::read_dir(path)
        .await
        .unwrap_or_else(|_| panic!("Directory should exist: {:?}", path));
    
    while let Some(entry) = entries.next_entry().await.expect("Read entry") {
        if entry.metadata().await.expect("Get metadata").is_file() {
            count += 1;
        }
    }
    
    assert_eq!(
        count, expected_count,
        "Directory file count mismatch for {:?}. Expected: {}, Got: {}",
        path, expected_count, count
    );
}

/// Проверяет, что директория пуста
pub async fn assert_directory_empty(path: &Path) {
    let mut entries = tokio::fs::read_dir(path)
        .await
        .unwrap_or_else(|_| panic!("Directory should exist: {:?}", path));
    
    let first_entry = entries.next_entry().await.expect("Read entry");
    assert!(
        first_entry.is_none(),
        "Directory should be empty: {:?}",
        path
    );
}

/// Проверяет, что директория не пуста
pub async fn assert_directory_not_empty(path: &Path) {
    let mut entries = tokio::fs::read_dir(path)
        .await
        .unwrap_or_else(|_| panic!("Directory should exist: {:?}", path));
    
    let first_entry = entries.next_entry().await.expect("Read entry");
    assert!(
        first_entry.is_some(),
        "Directory should not be empty: {:?}",
        path
    );
}

/// Проверяет минимальный размер файла (больше или равно)
pub async fn assert_file_min_size(path: &Path, min_size: u64) {
    let metadata = tokio::fs::metadata(path)
        .await
        .unwrap_or_else(|_| panic!("File should exist: {:?}", path));
    
    assert!(
        metadata.len() >= min_size,
        "File size should be at least {} bytes for {:?}, but got {}",
        min_size,
        path,
        metadata.len()
    );
}

/// Проверяет максимальный размер файла (меньше или равно)
pub async fn assert_file_max_size(path: &Path, max_size: u64) {
    let metadata = tokio::fs::metadata(path)
        .await
        .unwrap_or_else(|_| panic!("File should exist: {:?}", path));
    
    assert!(
        metadata.len() <= max_size,
        "File size should be at most {} bytes for {:?}, but got {}",
        max_size,
        path,
        metadata.len()
    );
}

/// Проверяет, что два файла идентичны
pub async fn assert_files_equal(path1: &Path, path2: &Path) {
    let content1 = tokio::fs::read(path1)
        .await
        .unwrap_or_else(|_| panic!("File should exist: {:?}", path1));
    let content2 = tokio::fs::read(path2)
        .await
        .unwrap_or_else(|_| panic!("File should exist: {:?}", path2));
    
    assert_eq!(
        content1, content2,
        "Files should be identical: {:?} and {:?}",
        path1, path2
    );
}

/// Проверяет, что содержимое файла содержит подстроку
pub async fn assert_file_contains(path: &Path, substring: &str) {
    let content = tokio::fs::read_to_string(path)
        .await
        .unwrap_or_else(|_| panic!("File should exist and be readable as UTF-8: {:?}", path));
    
    assert!(
        content.contains(substring),
        "File {:?} should contain substring '{}'",
        path,
        substring
    );
}

/// Проверяет права доступа к файлу (Unix-only)
#[cfg(unix)]
pub async fn assert_file_permissions(path: &Path, expected_mode: u32) {
    use std::os::unix::fs::PermissionsExt;
    
    let metadata = tokio::fs::metadata(path)
        .await
        .unwrap_or_else(|_| panic!("File should exist: {:?}", path));
    
    let mode = metadata.permissions().mode() & 0o777;
    assert_eq!(
        mode, expected_mode,
        "File permissions mismatch for {:?}. Expected: {:o}, Got: {:o}",
        path, expected_mode, mode
    );
}

/// Создает custom assertion с сообщением
#[macro_export]
macro_rules! assert_with_context {
    ($condition:expr, $context:expr) => {
        assert!($condition, "Assertion failed: {} - Context: {}", stringify!($condition), $context);
    };
    ($condition:expr, $context:expr, $($arg:tt)*) => {
        assert!($condition, "Assertion failed: {} - Context: {} - {}", 
                stringify!($condition), $context, format!($($arg)*));
    };
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;
    
    #[tokio::test]
    async fn test_assert_file_size() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("test.txt");
        tokio::fs::write(&file_path, b"hello").await.unwrap();
        
        assert_file_size(&file_path, 5).await;
    }
    
    #[tokio::test]
    async fn test_assert_file_content() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("test.txt");
        tokio::fs::write(&file_path, b"hello world").await.unwrap();
        
        assert_file_content(&file_path, b"hello world").await;
    }
    
    #[tokio::test]
    async fn test_assert_file_exists() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("test.txt");
        tokio::fs::write(&file_path, b"data").await.unwrap();
        
        assert_file_exists(&file_path).await;
    }
    
    #[tokio::test]
    async fn test_assert_directory_file_count() {
        let temp_dir = TempDir::new().unwrap();
        tokio::fs::write(temp_dir.path().join("file1.txt"), b"data")
            .await
            .unwrap();
        tokio::fs::write(temp_dir.path().join("file2.txt"), b"data")
            .await
            .unwrap();
        
        assert_directory_file_count(temp_dir.path(), 2).await;
    }
    
    #[tokio::test]
    async fn test_assert_file_min_max_size() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("test.txt");
        tokio::fs::write(&file_path, b"hello").await.unwrap();
        
        assert_file_min_size(&file_path, 5).await;
        assert_file_max_size(&file_path, 5).await;
        assert_file_min_size(&file_path, 1).await;
        assert_file_max_size(&file_path, 10).await;
    }
}