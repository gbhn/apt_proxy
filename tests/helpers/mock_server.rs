use mockito::{Mock, ServerGuard};

/// Builder для удобного создания и настройки mock-серверов
pub struct MockServerBuilder {
    server: ServerGuard,
}

impl MockServerBuilder {
    /// Создает новый mock-сервер
    pub async fn new() -> Self {
        Self {
            server: mockito::Server::new_async().await,
        }
    }
    
    /// Возвращает URL mock-сервера
    pub fn url(&self) -> String {
        self.server.url()
    }
    
    /// Создает простой GET mock с телом
    pub async fn mock_get(&mut self, path: &str, body: &[u8]) -> Mock {
        self.server
            .mock("GET", path)
            .with_status(200)
            .with_header("content-type", "application/octet-stream")
            .with_body(body)
            .create_async()
            .await
    }
    
    /// Создает GET mock с пользовательскими заголовками
    pub async fn mock_get_with_headers(
        &mut self,
        path: &str,
        body: &[u8],
        headers: Vec<(&str, &str)>,
    ) -> Mock {
        let mut mock = self.server.mock("GET", path).with_status(200);
        
        for (key, value) in headers {
            mock = mock.with_header(key, value);
        }
        
        mock.with_body(body).create_async().await
    }
    
    /// Создает GET mock с определенным статусом (без тела)
    pub async fn mock_get_status(&mut self, path: &str, status: u16) -> Mock {
        self.server
            .mock("GET", path)
            .with_status(status as usize)
            .create_async()
            .await
    }
    
    /// Создает GET mock, который возвращает debian-пакет
    pub async fn mock_debian_package(
        &mut self,
        path: &str,
        package_data: &[u8],
    ) -> Mock {
        self.mock_get_with_headers(
            path,
            package_data,
            vec![
                ("content-type", "application/x-debian-package"),
                ("content-length", &package_data.len().to_string()),
            ],
        )
        .await
    }
    
    /// Создает GET mock с ETag
    pub async fn mock_with_etag(
        &mut self,
        path: &str,
        body: &[u8],
        etag: &str,
    ) -> Mock {
        self.mock_get_with_headers(
            path,
            body,
            vec![
                ("content-type", "application/octet-stream"),
                ("etag", etag),
            ],
        )
        .await
    }
    
    /// Создает последовательность моков для имитации нестабильного соединения
    pub async fn mock_unstable_connection(
        &mut self,
        path: &str,
        failures_before_success: usize,
        success_body: &[u8],
    ) -> Vec<Mock> {
        let mut mocks = Vec::new();
        
        // Сначала создаем мокер с ошибками
        for _ in 0..failures_before_success {
            let mock = self.mock_get_status(path, 500).await;
            mocks.push(mock);
        }
        
        // Затем успешный ответ
        let mock = self.mock_get(path, success_body).await;
        mocks.push(mock);
        
        mocks
    }
    
    /// Создает mock с задержкой ответа
    pub async fn mock_with_delay(
        &mut self,
        path: &str,
        body: &[u8],
        delay_ms: u64,
    ) -> Mock {
        use std::time::Duration;
        
        self.server
            .mock("GET", path)
            .with_status(200)
            .with_header("content-type", "application/octet-stream")
            .with_body(body)
            .with_delay(Duration::from_millis(delay_ms))
            .create_async()
            .await
    }
    
    /// Извлекает внутренний ServerGuard для более сложных сценариев
    pub fn into_guard(self) -> ServerGuard {
        self.server
    }
    
    /// Получает ссылку на внутренний сервер
    pub fn server(&self) -> &ServerGuard {
        &self.server
    }
}

/// Быстрое создание простого mock-сервера для базовых тестов
pub async fn simple_mock_server(path: &str, body: &[u8]) -> (MockServerBuilder, Mock) {
    let mut builder = MockServerBuilder::new().await;
    let mock = builder.mock_get(path, body).await;
    (builder, mock)
}

/// Создает mock-сервер с несколькими эндпоинтами
pub async fn multi_endpoint_mock(endpoints: Vec<(&str, &[u8])>) -> MockServerBuilder {
    let mut builder = MockServerBuilder::new().await;
    
    for (path, body) in endpoints {
        builder.mock_get(path, body).await;
    }
    
    builder
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[tokio::test]
    async fn test_mock_server_builder() {
        let mut mock = MockServerBuilder::new().await;
        let url = mock.url();
        
        assert!(!url.is_empty());
        assert!(url.starts_with("http://"));
    }
    
    #[tokio::test]
    async fn test_simple_mock_creation() {
        let (mock_server, _mock) = simple_mock_server("/test", b"test data").await;
        
        let client = reqwest::Client::new();
        let response = client
            .get(format!("{}/test", mock_server.url()))
            .send()
            .await
            .unwrap();
        
        assert_eq!(response.status(), 200);
        let body = response.bytes().await.unwrap();
        assert_eq!(body.as_ref(), b"test data");
    }
}