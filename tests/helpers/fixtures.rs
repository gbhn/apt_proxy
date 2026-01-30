use apt_cacher_rs::config::Settings;
use std::collections::HashMap;
use std::path::PathBuf;
use tempfile::TempDir;

/// Создает временную директорию для тестов
pub fn temp_dir() -> TempDir {
    TempDir::new().expect("Failed to create temp directory")
}

/// Базовые настройки для тестирования с заданной директорией кеша
pub fn basic_settings(cache_dir: PathBuf) -> Settings {
    Settings {
        port: 3142,
        socket: None,
        repositories: HashMap::new(),
        cache_dir,
        max_cache_size: 10 * 1024 * 1024, // 10 MB
        max_lru_entries: 100,
    }
}

/// Минимальные настройки для тестов с ограниченными ресурсами
pub fn minimal_settings(cache_dir: PathBuf) -> Settings {
    Settings {
        port: 3142,
        socket: None,
        repositories: HashMap::new(),
        cache_dir,
        max_cache_size: 1024 * 1024, // 1 MB
        max_lru_entries: 10,
    }
}

/// Настройки с предустановленным репозиторием
pub fn settings_with_repo(
    cache_dir: PathBuf,
    repo_name: &str,
    repo_url: &str,
) -> Settings {
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

/// Настройки с несколькими репозиториями
pub fn settings_with_multiple_repos(
    cache_dir: PathBuf,
    repos: Vec<(&str, &str)>,
) -> Settings {
    let mut repositories = HashMap::new();
    for (name, url) in repos {
        repositories.insert(name.to_string(), url.to_string());
    }
    
    Settings {
        port: 3142,
        socket: None,
        repositories,
        cache_dir,
        max_cache_size: 10 * 1024 * 1024,
        max_lru_entries: 100,
    }
}

/// Настройки с пользовательскими параметрами кеша
pub fn settings_with_cache_params(
    cache_dir: PathBuf,
    max_size: u64,
    max_entries: usize,
) -> Settings {
    Settings {
        port: 3142,
        socket: None,
        repositories: HashMap::new(),
        cache_dir,
        max_cache_size: max_size,
        max_lru_entries: max_entries,
    }
}

/// Builder для более гибкой настройки
pub struct SettingsBuilder {
    port: u16,
    socket: Option<PathBuf>,
    repositories: HashMap<String, String>,
    cache_dir: PathBuf,
    max_cache_size: u64,
    max_lru_entries: usize,
}

impl SettingsBuilder {
    pub fn new(cache_dir: PathBuf) -> Self {
        Self {
            port: 3142,
            socket: None,
            repositories: HashMap::new(),
            cache_dir,
            max_cache_size: 10 * 1024 * 1024,
            max_lru_entries: 100,
        }
    }
    
    pub fn port(mut self, port: u16) -> Self {
        self.port = port;
        self
    }
    
    pub fn socket(mut self, socket: PathBuf) -> Self {
        self.socket = Some(socket);
        self
    }
    
    pub fn add_repository(mut self, name: &str, url: &str) -> Self {
        self.repositories.insert(name.to_string(), url.to_string());
        self
    }
    
    pub fn cache_size(mut self, size: u64) -> Self {
        self.max_cache_size = size;
        self
    }
    
    pub fn lru_entries(mut self, entries: usize) -> Self {
        self.max_lru_entries = entries;
        self
    }
    
    pub fn build(self) -> Settings {
        Settings {
            port: self.port,
            socket: self.socket,
            repositories: self.repositories,
            cache_dir: self.cache_dir,
            max_cache_size: self.max_cache_size,
            max_lru_entries: self.max_lru_entries,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_builder_pattern() {
        let temp = temp_dir();
        let settings = SettingsBuilder::new(temp.path().to_path_buf())
            .port(8080)
            .cache_size(5 * 1024 * 1024)
            .add_repository("ubuntu", "http://archive.ubuntu.com/ubuntu")
            .build();
        
        assert_eq!(settings.port, 8080);
        assert_eq!(settings.max_cache_size, 5 * 1024 * 1024);
        assert_eq!(settings.repositories.len(), 1);
    }
}