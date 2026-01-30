/// Общий модуль с утилитами для всех тестов
/// 
/// Этот модуль содержит переиспользуемые компоненты:
/// - Создание тестовых настроек и окружений
/// - Mock-серверы и их конфигурация
/// - Утилиты для работы с файлами
/// - Assertion helpers

pub mod fixtures;
pub mod mock_server;
pub mod file_utils;
pub mod assertions;

// Re-export часто используемых типов
pub use fixtures::*;
pub use mock_server::*;
pub use file_utils::*;
pub use assertions::*;