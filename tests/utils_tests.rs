use apt_cacher_rs::utils::*;

#[test]
fn test_format_size_bytes() {
    assert_eq!(format_size(0), "0 B");
    assert_eq!(format_size(1), "1 B");
    assert_eq!(format_size(999), "999 B");
}

#[test]
fn test_format_size_kilobytes() {
    assert_eq!(format_size(1024), "1.00 KB");
    assert_eq!(format_size(1536), "1.50 KB");
    assert_eq!(format_size(2048), "2.00 KB");
}

#[test]
fn test_format_size_megabytes() {
    assert_eq!(format_size(1_048_576), "1.00 MB");
    assert_eq!(format_size(5_242_880), "5.00 MB");
    assert_eq!(format_size(10_737_418), "10.24 MB");
}

#[test]
fn test_format_size_gigabytes() {
    assert_eq!(format_size(1_073_741_824), "1.00 GB");
    assert_eq!(format_size(5_368_709_120), "5.00 GB");
    assert_eq!(format_size(10_737_418_240), "10.00 GB");
}

#[test]
fn test_validate_path_valid() {
    assert!(validate_path("ubuntu/dists/focal/main").is_ok());
    assert!(validate_path("debian/pool/main/a/apt/apt_2.0.deb").is_ok());
    assert!(validate_path("a/b/c/d/e/f/g").is_ok());
    assert!(validate_path("file-name_with.special-chars").is_ok());
}

#[test]
fn test_validate_path_empty() {
    let result = validate_path("");
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("length"));
}

#[test]
fn test_validate_path_too_long() {
    let long_path = "a".repeat(3000);
    let result = validate_path(&long_path);
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("length"));
}

#[test]
fn test_validate_path_with_null_bytes() {
    let result = validate_path("path\0with\0nulls");
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("invalid"));
}

#[test]
fn test_validate_path_with_parent_traversal() {
    assert!(validate_path("../etc/passwd").is_err());
    assert!(validate_path("path/../../../etc/shadow").is_err());
    assert!(validate_path("normal/path/../hack").is_err());
}

#[test]
fn test_cache_path_deterministic() {
    let base = std::path::Path::new("/cache");
    let path1 = cache_path_for(base, "ubuntu/dists/focal/Release");
    let path2 = cache_path_for(base, "ubuntu/dists/focal/Release");
    
    assert_eq!(path1, path2);
}

#[test]
fn test_cache_path_different_inputs() {
    let base = std::path::Path::new("/cache");
    let path1 = cache_path_for(base, "ubuntu/dists/focal/Release");
    let path2 = cache_path_for(base, "debian/dists/stable/Release");
    
    assert_ne!(path1, path2);
}

#[test]
fn test_cache_path_structure() {
    let base = std::path::Path::new("/cache");
    let path = cache_path_for(base, "test/path");
    
    let components: Vec<_> = path.components().collect();
    
    assert!(components.len() >= 4);
    assert_eq!(components[0].as_os_str(), "/");
    assert_eq!(components[1].as_os_str(), "cache");
    
    let dir1 = components[2].as_os_str().to_str().unwrap();
    let dir2 = components[3].as_os_str().to_str().unwrap();
    assert_eq!(dir1.len(), 2);
    assert_eq!(dir2.len(), 2);
}

#[test]
fn test_headers_path_for() {
    let cache_path = std::path::Path::new("/cache/aa/bb/ccddee");
    let headers_path = headers_path_for(cache_path);
    
    assert_eq!(
        headers_path,
        std::path::PathBuf::from("/cache/aa/bb/ccddee.headers")
    );
}

#[test]
fn test_part_path_for() {
    let cache_path = std::path::Path::new("/cache/aa/bb/ccddee");
    let part_path = part_path_for(cache_path);
    
    assert_eq!(
        part_path.to_str().unwrap(),
        "/cache/aa/bb/ccddee.part"
    );
}

#[test]
fn test_cache_path_collision_resistance() {
    let base = std::path::Path::new("/cache");
    
    let paths = vec![
        "ubuntu/dists/focal/main",
        "ubuntu/dists/focal/main/",
        "ubuntu/dists/focal/main/binary-amd64",
        "debian/dists/focal/main",
    ];
    
    let mut generated_paths = std::collections::HashSet::new();
    for path in paths {
        let cache_path = cache_path_for(base, path);
        assert!(
            generated_paths.insert(cache_path),
            "Collision detected for path: {}",
            path
        );
    }
}

#[cfg(feature = "proptest")]
mod property_tests {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        #[test]
        fn test_validate_path_never_panics(s in "\\PC*") {
            let _ = validate_path(&s);
        }

        #[test]
        fn test_format_size_always_valid(size in 0u64..1_000_000_000_000u64) {
            let formatted = format_size(size);
            assert!(!formatted.is_empty());
            assert!(
                formatted.contains("B") || 
                formatted.contains("KB") || 
                formatted.contains("MB") || 
                formatted.contains("GB")
            );
        }

        #[test]
        fn test_cache_path_always_valid(s in "[a-zA-Z0-9/_-]{1,100}") {
            let base = std::path::Path::new("/cache");
            let path = cache_path_for(base, &s);
            assert!(path.starts_with(base));
        }
    }
}