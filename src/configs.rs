use std::path::{Path, PathBuf};
use std::sync::LazyLock;

use serde::Deserialize;
use toml::Table;
/// Global configuration loaded from a TOML file.
///
/// This static variable holds the application configuration, which is lazily loaded
/// from either a "dataloader.toml" file in the current directory or a "config.toml"
/// file in the project's root directory.
pub static CONFIG: LazyLock<Config> = LazyLock::new(|| {
    let config_str = if Path::new("dataloader.toml").exists() {
        std::fs::read_to_string("dataloader.toml").unwrap()
    } else {
        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("config.toml");
        std::fs::read_to_string(path).unwrap()
    };
    toml::from_str(&config_str).unwrap()
});

/// Main configuration structure.
///
/// This struct represents the top-level configuration for the application,
/// containing sub-configurations for path finding and data loading.
#[derive(Deserialize, Clone)]
pub(crate) struct Config {
    /// Configuration for path finding.
    pub path_finder: MainPathConfig,
    /// Configuration for data loading.
    pub loader: LoaderConfig,
}

/// Configuration for data loading.
///
/// This struct contains settings related to data loading operations,
/// such as renaming rules.
#[derive(Deserialize, Clone)]
pub(crate) struct LoaderConfig {
    /// A table of renaming rules for data fields.
    pub rename: Table,
}

/// Configuration for main path settings.
///
/// This struct contains settings related to path finding and type sources.
#[derive(Deserialize, Clone, Default)]
pub(crate) struct MainPathConfig {
    /// A table of main paths used in the application.
    pub main_path: Table,
    /// A table mapping types to their source locations.
    pub type_source: Table,
}
