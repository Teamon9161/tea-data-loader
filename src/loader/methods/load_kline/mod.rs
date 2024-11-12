mod future;
mod opt;
mod sse_bond;
mod xbond;

use anyhow::bail;
pub use opt::KlineOpt;
use toml::{Table, Value};

use crate::prelude::*;
use crate::utils::get_time_filter_cond;

/// Data loading and processing methods.
impl DataLoader {
    /// Generates a time filter condition based on the given frequency.
    ///
    /// # Arguments
    ///
    /// * `freq` - The frequency of the data.
    ///
    /// # Returns
    ///
    /// Returns a `Result` containing an `Option<Expr>` representing the time filter condition.
    #[inline]
    pub fn time_filter_cond(&self, freq: &str) -> Result<Option<Expr>> {
        if freq == "min" || freq.contains("compose_") {
            Ok(get_time_filter_cond(self.start, self.end, "trading_date"))
        } else if freq == "daily" {
            Ok(get_time_filter_cond(self.start, self.end, "date"))
        } else if freq == "tick" {
            Ok(get_time_filter_cond(self.start, self.end, "time"))
        } else {
            bail!("Unsupported freq: {}", freq)
        }
    }

    /// Retrieves the rename table for the given tier.
    ///
    /// # Arguments
    ///
    /// * `tier` - The tier of the data.
    ///
    /// # Returns
    ///
    /// Returns an `Option<Table>` containing the rename configuration.
    #[inline]
    pub fn rename_table(&self, tier: Tier) -> Option<Table> {
        let rename_config = &CONFIG.loader.rename;
        parse_rename_config(
            rename_config,
            Some(self.typ.as_ref()),
            Some(self.freq.as_deref().unwrap()),
            Some(tier.as_str()),
        )
    }

    /// Loads kline data based on the given options.
    ///
    /// # Arguments
    ///
    /// * `opt` - The `KlineOpt` containing the loading options.
    ///
    /// # Returns
    ///
    /// Returns a `Result<Self>` containing the updated `DataLoader`.
    pub fn kline(mut self, opt: KlineOpt) -> Result<Self> {
        let path_config = opt.path_config(&self.typ);
        self.freq = Some(opt.freq.into());
        match self.typ.as_ref() {
            "future" | "ddb-future" => self.load_future_kline(path_config),
            "xbond" | "ddb-xbond" => self.load_xbond_kline(path_config, opt.concat_tick_df),
            "sse-bond" => self.load_sse_bond_kline(path_config),
            _ => bail!("Load Unsupported typ: {:?} kline", self.typ),
        }
    }
}

/// Extracts a rename map from a TOML configuration.
///
/// # Arguments
///
/// * `config` - An optional TOML `Value` containing the rename configuration.
///
/// # Returns
///
/// Returns an `Option<Table>` containing the extracted rename map.
#[inline]
fn get_rename_map(config: Option<&Value>) -> Option<Table> {
    if let Some(config) = config {
        if let Some(config) = config.as_table() {
            let mut map = Table::new();
            for (k, v) in config.iter() {
                if !v.is_table() {
                    map.insert(k.clone(), v.clone());
                }
            }
            if !map.is_empty() {
                return Some(map);
            }
        }
    }
    None
}

/// Parses the rename configuration based on the given parameters.
///
/// # Arguments
///
/// * `config` - The TOML `Table` containing the rename configuration.
/// * `typ` - An optional string representing the data type.
/// * `freq` - An optional string representing the data frequency.
/// * `tier` - An optional string representing the data tier.
///
/// # Returns
///
/// Returns an `Option<Table>` containing the parsed rename configuration.
pub(crate) fn parse_rename_config(
    config: &Table,
    typ: Option<&str>,
    freq: Option<&str>,
    tier: Option<&str>,
) -> Option<Table> {
    match (typ, freq, tier) {
        (Some(typ), Some(freq), Some(tier)) => {
            if let Some(map) = config.get(typ) {
                if let Some(map) = map.get(freq) {
                    if let Some(map) = map.get(tier) {
                        map.as_table().cloned()
                    } else {
                        parse_rename_config(config, Some(typ), Some(freq), None)
                    }
                } else {
                    parse_rename_config(config, Some(typ), None, None)
                }
            } else {
                parse_rename_config(config, None, None, None)
            }
        },
        (Some(typ), Some(freq), None) => {
            if let Some(map) = config.get(typ) {
                if let Some(map) = get_rename_map(map.get(freq)) {
                    Some(map.clone())
                } else {
                    parse_rename_config(config, Some(typ), None, None)
                }
            } else {
                parse_rename_config(config, None, None, None)
            }
        },
        (Some(typ), None, None) => get_rename_map(config.get(typ)),
        _ => None,
    }
}
