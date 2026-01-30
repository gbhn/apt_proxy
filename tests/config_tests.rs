use apt_cacher_rs::config::*;
use std::path::PathBuf;
use std::io::Write; 
use tempfile::NamedTempFile;

#[test]
fn test_parse_size_gigabytes() {
    assert_eq!(ConfigFile::parse_size("10GB"), Some(10_737_418_240));
    assert_eq!(ConfigFile::parse_size("5 GB"), Some(5_368_709_120));
    assert_eq!(ConfigFile::parse_size("1gb"), Some(1_073_741_824));
}

#[test]
fn test_parse_size_megabytes() {
    assert_eq!(ConfigFile::parse_size("500MB"), Some(524_288_000));
    assert_eq!(ConfigFile::parse_size("100 MB"), Some(104_857_600));
}

#[test]
fn test_parse_size_kilobytes() {
    assert_eq!(ConfigFile::parse_size("1024KB"), Some(1_048_576));
    assert_eq!(ConfigFile::parse_size("512 KB"), Some(524_288));
}

#[test]
fn test_parse_size_bytes() {
    assert_eq!(ConfigFile::parse_size("2048"), Some(2048));
    assert_eq!(ConfigFile::parse_size("1024B"), Some(1024));
}

#[test]
fn test_parse_size_invalid() {
    assert_eq!(ConfigFile::parse_size("invalid"), None);
    assert_eq!(ConfigFile::parse_size("10XB"), None);
    assert_eq!(ConfigFile::parse_size(""), None);
}

#[test]
fn test_parse_size_case_insensitive() {
    assert_eq!(ConfigFile::parse_size("10GB"), ConfigFile::parse_size("10gb"));
    assert_eq!(ConfigFile::parse_size("500MB"), ConfigFile::parse_size("500Mb"));
}

#[tokio::test]
async fn test_settings_defaults() {
    let mut temp_file = NamedTempFile::new().unwrap();
    write!(temp_file, "{{}}").unwrap(); 

    let args = Args {
        config: Some(temp_file.path().to_path_buf()),
        port: None,
        socket: None,
        cache_dir: None,
        max_cache_size: None,
        max_lru_entries: None,
    };

    let settings = Settings::load(args).await.unwrap();

    assert_eq!(settings.port, 3142);
    assert_eq!(settings.cache_dir, PathBuf::from("./apt_cache"));
}

#[tokio::test]
async fn test_settings_cli_overrides() {
    let mut temp_file = NamedTempFile::new().unwrap();
    write!(temp_file, "{{}}").unwrap();

    let args = Args {
        config: Some(temp_file.path().to_path_buf()),
        port: Some(8080),
        socket: None,
        cache_dir: Some(PathBuf::from("/custom/cache")),
        max_cache_size: Some(5_368_709_120),
        max_lru_entries: Some(50_000),
    };

    let settings = Settings::load(args).await.unwrap();

    assert_eq!(settings.port, 8080);
    assert_eq!(settings.cache_dir, PathBuf::from("/custom/cache"));
    assert_eq!(settings.max_cache_size, 5_368_709_120);
}

#[tokio::test]
async fn test_load_yaml_config() {
    let mut temp_file = NamedTempFile::new().unwrap();
    let config_content = r#"
port: 9000
cache_dir: /tmp/test_cache
max_cache_size_human: "5 GB"
max_lru_entries: 50000
repositories:
  ubuntu: "http://archive.ubuntu.com/ubuntu"
  debian: "http://deb.debian.org/debian"
"#;
    
    temp_file.write_all(config_content.as_bytes()).unwrap();
    temp_file.flush().unwrap();

    let args = Args {
        config: Some(temp_file.path().to_path_buf()),
        port: None,
        socket: None,
        cache_dir: None,
        max_cache_size: None,
        max_lru_entries: None,
    };

    let settings = Settings::load(args).await.unwrap();

    assert_eq!(settings.port, 9000);
    assert_eq!(settings.cache_dir, PathBuf::from("/tmp/test_cache"));
    assert_eq!(settings.max_cache_size, 5_368_709_120);
    assert_eq!(settings.repositories.len(), 2);
}

#[tokio::test]
async fn test_config_cli_overrides_file() {
    let mut temp_file = NamedTempFile::new().unwrap();
    let config_content = r#"
port: 9000
cache_dir: /tmp/test_cache
max_cache_size_human: "5 GB"
"#;
    
    temp_file.write_all(config_content.as_bytes()).unwrap();
    temp_file.flush().unwrap();

    let args = Args {
        config: Some(temp_file.path().to_path_buf()),
        port: Some(8080),
        socket: None,
        cache_dir: None,
        max_cache_size: Some(1_073_741_824),
        max_lru_entries: None,
    };

    let settings = Settings::load(args).await.unwrap();

    assert_eq!(settings.port, 8080);
    assert_eq!(settings.max_cache_size, 1_073_741_824);
    assert_eq!(settings.cache_dir, PathBuf::from("/tmp/test_cache"));
}

#[tokio::test]
async fn test_invalid_yaml_config() {
    let mut temp_file = NamedTempFile::new().unwrap();
    let config_content = r#"
invalid yaml content: [
  unclosed bracket
"#;
    
    temp_file.write_all(config_content.as_bytes()).unwrap();
    temp_file.flush().unwrap();

    let args = Args {
        config: Some(temp_file.path().to_path_buf()),
        port: None,
        socket: None,
        cache_dir: None,
        max_cache_size: None,
        max_lru_entries: None,
    };

    let result = Settings::load(args).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_nonexistent_config_file() {
    let args = Args {
        config: Some(PathBuf::from("/nonexistent/config.yaml")),
        port: None,
        socket: None,
        cache_dir: None,
        max_cache_size: None,
        max_lru_entries: None,
    };

    let result = Settings::load(args).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_config_with_socket() {
    let mut temp_file = NamedTempFile::new().unwrap();
    let config_content = r#"
socket: /var/run/apt-cacher.sock
"#;
    
    temp_file.write_all(config_content.as_bytes()).unwrap();
    temp_file.flush().unwrap();

    let args = Args {
        config: Some(temp_file.path().to_path_buf()),
        port: None,
        socket: None,
        cache_dir: None,
        max_cache_size: None,
        max_lru_entries: None,
    };

    let settings = Settings::load(args).await.unwrap();
    assert_eq!(settings.socket, Some(PathBuf::from("/var/run/apt-cacher.sock")));
}

#[tokio::test]
async fn test_human_size_priority() {
    let mut temp_file = NamedTempFile::new().unwrap();
    let config_content = r#"
max_cache_size: 1000000000
max_cache_size_human: "5 GB"
"#;
    
    temp_file.write_all(config_content.as_bytes()).unwrap();
    temp_file.flush().unwrap();

    let args = Args {
        config: Some(temp_file.path().to_path_buf()),
        port: None,
        socket: None,
        cache_dir: None,
        max_cache_size: None,
        max_lru_entries: None,
    };

    let settings = Settings::load(args).await.unwrap();
    assert_eq!(settings.max_cache_size, 5_368_709_120);
}

#[test]
fn test_display_info_no_panic() {
    let settings = Settings {
        port: 3142,
        socket: None,
        repositories: std::collections::HashMap::new(),
        cache_dir: PathBuf::from("/tmp"),
        max_cache_size: 10_737_418_240,
        max_lru_entries: 100_000,
    };
    settings.display_info();
}