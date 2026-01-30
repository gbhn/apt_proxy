use apt_cacher_rs::{cache::*, config::Settings, utils, build_router, AppState};
use criterion::{black_box, criterion_group, criterion_main, Criterion, BenchmarkId, Throughput};
use std::path::Path;
use std::sync::Arc;
use tempfile::TempDir;
use axum::{
    body::Body,
    http::Request,
};
use tower::ServiceExt; 

fn create_bench_settings(temp_dir: &TempDir) -> Settings {
    Settings {
        port: 3142,
        socket: None,
        repositories: std::collections::HashMap::new(),
        cache_dir: temp_dir.path().to_path_buf(),
        max_cache_size: 100 * 1024 * 1024,
        max_lru_entries: 1000,
    }
}


fn bench_cache_path_generation(c: &mut Criterion) {
    let base = Path::new("/cache");
    
    let paths = vec![
        "ubuntu/dists/focal/Release",
        "debian/pool/main/a/apt/apt_2.0.deb",
        "security/dists/focal-security/InRelease",
        "ubuntu/dists/jammy/main/binary-amd64/Packages.gz",
    ];
    
    c.bench_function("cache_path_generation", |b| {
        b.iter(|| {
            for path in &paths {
                black_box(utils::cache_path_for(base, path));
            }
        });
    });
}

fn bench_format_size(c: &mut Criterion) {
    let sizes = vec![
        1024,
        1_048_576,
        10_485_760,
        1_073_741_824,
        10_737_418_240,
    ];
    
    c.bench_function("format_size", |b| {
        b.iter(|| {
            for size in &sizes {
                black_box(utils::format_size(*size));
            }
        });
    });
}

fn bench_validate_path(c: &mut Criterion) {
    let paths = vec![
        "ubuntu/dists/focal/main",
        "debian/pool/main/a/apt/apt_2.0.deb",
        "a/b/c/d/e/f/g/h/i/j/k/l/m/n/o/p",
        "file-name_with.special-chars-123",
    ];
    
    c.bench_function("validate_path", |b| {
        b.iter(|| {
            for path in &paths {
                black_box(utils::validate_path(path)).ok();
            }
        });
    });
}

fn bench_cache_metadata_serialization(c: &mut Criterion) {
    let mut headers = axum::http::HeaderMap::new();
    headers.insert("content-type", "application/x-debian-package".parse().unwrap());
    headers.insert("content-length", "1234567".parse().unwrap());
    headers.insert("etag", "\"abc123def456\"".parse().unwrap());
    headers.insert("last-modified", "Mon, 01 Jan 2024 00:00:00 GMT".parse().unwrap());
    
    let meta = CacheMetadata {
        headers,
        original_url_path: "ubuntu/pool/main/a/apt/apt_2.0.9_amd64.deb".to_string(),
    };
    
    c.bench_function("cache_metadata_serialization", |b| {
        b.iter(|| {
            black_box(serde_json::to_string(&meta).unwrap());
        });
    });
}

fn bench_cache_metadata_deserialization(c: &mut Criterion) {
    let mut headers = axum::http::HeaderMap::new();
    headers.insert("content-type", "application/x-debian-package".parse().unwrap());
    headers.insert("content-length", "1234567".parse().unwrap());
    
    let meta = CacheMetadata {
        headers,
        original_url_path: "ubuntu/pool/main/a/apt/apt_2.0.9_amd64.deb".to_string(),
    };
    
    let json = serde_json::to_string(&meta).unwrap();
    
    c.bench_function("cache_metadata_deserialization", |b| {
        b.iter(|| {
            black_box(serde_json::from_str::<CacheMetadata>(&json).unwrap());
        });
    });
}

fn bench_cache_store_small(c: &mut Criterion) {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let temp_dir = TempDir::new().unwrap();
    let settings = create_bench_settings(&temp_dir);
    
    let cache = runtime.block_on(async {
        CacheManager::new(settings).await.unwrap()
    });
    
    let data = vec![0u8; 1024]; 
    let mut headers = axum::http::HeaderMap::new();
    headers.insert("content-type", "text/plain".parse().unwrap());
    
    let meta = CacheMetadata {
        headers,
        original_url_path: "test/small.txt".to_string(),
    };
    
    c.bench_function("cache_store_1kb", |b| {
        b.to_async(&runtime).iter(|| async {
            cache.store("test/small.txt", &data, &meta).await.unwrap();
        });
    });
}

fn bench_cache_store_various_sizes(c: &mut Criterion) {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("cache_store_sizes");
    
    for size in [1024, 10_240, 102_400, 1_048_576].iter() {
        let temp_dir = TempDir::new().unwrap();
        let settings = create_bench_settings(&temp_dir);
        
        let cache = runtime.block_on(async {
            CacheManager::new(settings).await.unwrap()
        });
        
        let data = vec![0u8; *size];
        let mut headers = axum::http::HeaderMap::new();
        headers.insert("content-type", "application/octet-stream".parse().unwrap());
        
        let meta = CacheMetadata {
            headers,
            original_url_path: format!("test/file_{}.bin", size),
        };
        
        group.throughput(Throughput::Bytes(*size as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(size),
            size,
            |b, &size| {
                b.to_async(&runtime).iter(|| async {
                    cache.store(
                        &format!("test/file_{}.bin", size),
                        &data,
                        &meta
                    ).await.unwrap();
                });
            },
        );
    }
    
    group.finish();
}

fn bench_cache_serve_hit(c: &mut Criterion) {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let temp_dir = TempDir::new().unwrap();
    let settings = create_bench_settings(&temp_dir);
    
    let cache = runtime.block_on(async {
        let cache = CacheManager::new(settings).await.unwrap();
        
        let data = vec![0u8; 10_240];
        let mut headers = axum::http::HeaderMap::new();
        headers.insert("content-type", "text/plain".parse().unwrap());
        
        let meta = CacheMetadata {
            headers,
            original_url_path: "test/cached.txt".to_string(),
        };
        
        cache.store("test/cached.txt", &data, &meta).await.unwrap();
        cache
    });
    
    c.bench_function("cache_serve_hit", |b| {
        b.to_async(&runtime).iter(|| async {
            black_box(cache.serve_cached("test/cached.txt").await.unwrap());
        });
    });
}

fn bench_cache_serve_miss(c: &mut Criterion) {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let temp_dir = TempDir::new().unwrap();
    let settings = create_bench_settings(&temp_dir);
    
    let cache = runtime.block_on(async {
        CacheManager::new(settings).await.unwrap()
    });
    
    c.bench_function("cache_serve_miss", |b| {
        b.to_async(&runtime).iter(|| async {
            black_box(cache.serve_cached("test/nonexistent.txt").await.unwrap());
        });
    });
}

fn bench_concurrent_cache_access(c: &mut Criterion) {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let temp_dir = TempDir::new().unwrap();
    let settings = create_bench_settings(&temp_dir);
    
    let cache = std::sync::Arc::new(runtime.block_on(async {
        let cache = CacheManager::new(settings).await.unwrap();
        
        for i in 0..10 {
            let data = vec![0u8; 1024];
            let mut headers = axum::http::HeaderMap::new();
            headers.insert("content-type", "text/plain".parse().unwrap());
            
            let meta = CacheMetadata {
                headers,
                original_url_path: format!("test/file{}.txt", i),
            };
            
            cache.store(&format!("test/file{}.txt", i), &data, &meta)
                .await
                .unwrap();
        }
        
        cache
    }));
    
    c.bench_function("concurrent_cache_access", |b| {
        b.to_async(&runtime).iter(|| {
            let cache_clone = cache.clone();
            async move {
                let mut handles = vec![];
                
                for i in 0..10 {
                    let cache = cache_clone.clone();
                    let handle = tokio::spawn(async move {
                        cache.serve_cached(&format!("test/file{}.txt", i))
                            .await
                            .unwrap()
                    });
                    handles.push(handle);
                }
                
                for handle in handles {
                    black_box(handle.await.unwrap());
                }
            }
        });
    });
}


fn bench_lru_eviction_pressure(c: &mut Criterion) {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let temp_dir = TempDir::new().unwrap();
    
    let settings = Settings {
        port: 3142,
        socket: None,
        repositories: std::collections::HashMap::new(),
        cache_dir: temp_dir.path().to_path_buf(),
        max_cache_size: 10 * 1024 * 1024,
        max_lru_entries: 50, 
    };
    
    let cache = runtime.block_on(async {
        let c = CacheManager::new(settings).await.unwrap();
        for i in 0..50 {
            let data = vec![0u8; 100];
            let mut headers = axum::http::HeaderMap::new();
            headers.insert("content-type", "text/plain".parse().unwrap());
            let meta = CacheMetadata { headers, original_url_path: format!("file_{}", i) };
            c.store(&format!("file_{}", i), &data, &meta).await.unwrap();
        }
        c
    });

    let mut counter = 50;

    c.bench_function("lru_eviction_pressure", |b| {
        b.to_async(&runtime).iter(|| {
            let idx = counter;
            counter += 1; 
            
            let path = format!("evict_file_{}", idx); 
            let data = vec![0u8; 100];
            let mut headers = axum::http::HeaderMap::new();
            headers.insert("content-type", "text/plain".parse().unwrap());
            let meta = CacheMetadata { headers, original_url_path: path.clone() };

            let cache_ref = &cache;
            async move {
                cache_ref.store(&path, &data, &meta).await.unwrap();
            }
        });
    });
}

fn bench_e2e_cache_hit(c: &mut Criterion) {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let temp_dir = TempDir::new().unwrap();
    
    let mut repositories = std::collections::HashMap::new();
    repositories.insert("ubuntu".to_string(), "http://archive.ubuntu.com/ubuntu".to_string());

    let settings = Settings {
        port: 3142,
        socket: None,
        repositories,
        cache_dir: temp_dir.path().to_path_buf(),
        max_cache_size: 100 * 1024 * 1024,
        max_lru_entries: 1000,
    };

    let app = runtime.block_on(async {
        let cache = CacheManager::new(settings.clone()).await.unwrap();
        
        let data = b"content";
        let mut headers = axum::http::HeaderMap::new();
        headers.insert("content-type", "text/plain".parse().unwrap());
        let meta = CacheMetadata { 
            headers, 
            original_url_path: "ubuntu/test-pkg.deb".to_string() 
        };
        cache.store("ubuntu/test-pkg.deb", data, &meta).await.unwrap();

        let state = Arc::new(AppState::new(settings, cache));
        let app = build_router(state);
        app
    });

    c.bench_function("e2e_cache_hit_router", |b| {
        b.to_async(&runtime).iter(|| {
            let app = app.clone();
            async move {
                let req = Request::builder()
                    .uri("/ubuntu/test-pkg.deb")
                    .body(Body::empty())
                    .unwrap();
                
                let resp = app.oneshot(req).await.unwrap();
                assert_eq!(resp.status(), 200);
                black_box(resp);
            }
        });
    });
}

fn bench_high_concurrency_reads(c: &mut Criterion) {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let temp_dir = TempDir::new().unwrap();
    let settings = create_bench_settings(&temp_dir);
    
    let cache = std::sync::Arc::new(runtime.block_on(async {
        let cache = CacheManager::new(settings).await.unwrap();
        let data = vec![0u8; 1024];
        let mut headers = axum::http::HeaderMap::new();
        headers.insert("content-type", "text/plain".parse().unwrap());
        let meta = CacheMetadata {
            headers,
            original_url_path: "shared.file".to_string(),
        };
        cache.store("shared.file", &data, &meta).await.unwrap();
        cache
    }));

    let mut group = c.benchmark_group("high_concurrency");
    group.sample_size(50); 

    group.bench_function("50_concurrent_reads", |b| {
        b.to_async(&runtime).iter(|| {
            let cache = cache.clone();
            async move {
                let mut handles = Vec::with_capacity(50);
                for _ in 0..50 {
                    let c = cache.clone();
                    handles.push(tokio::spawn(async move {
                        c.serve_cached("shared.file").await.unwrap()
                    }));
                }
                
                for h in handles {
                    black_box(h.await.unwrap());
                }
            }
        });
    });
    group.finish();
}

criterion_group!(
    benches,
    bench_cache_path_generation,
    bench_format_size,
    bench_validate_path,
    bench_cache_metadata_serialization,
    bench_cache_metadata_deserialization,
    bench_cache_store_small,
    bench_cache_store_various_sizes,
    bench_cache_serve_hit,
    bench_cache_serve_miss,
    bench_concurrent_cache_access,
    bench_lru_eviction_pressure,   
    bench_e2e_cache_hit,           
    bench_high_concurrency_reads   
);

criterion_main!(benches);