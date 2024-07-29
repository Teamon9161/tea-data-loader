use std::sync::LazyLock;

use serde::Deserialize;
use toml::Table;

pub static CONFIG: LazyLock<Config> = LazyLock::new(|| {
    let config_str = std::fs::read_to_string("config.toml").unwrap();
    toml::from_str(&config_str).unwrap()
});

#[derive(Deserialize, Clone)]
pub(crate) struct Config {
    pub path_finder: MainPathConfig,
    pub loader: LoaderConfig,
}

#[derive(Deserialize, Clone)]
pub(crate) struct LoaderConfig {
    pub rename: Table,
}

#[derive(Deserialize, Clone)]
pub(crate) struct MainPathConfig {
    pub main_path: Table,
    pub type_source: Table,
}
