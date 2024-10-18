use crate::path_finder::PathConfig;
use crate::prelude::*;

/// Options for loading kline data.
///
/// This struct provides configuration options for loading kline (candlestick) data
/// in a DataLoader.
#[derive(Clone, Debug, Copy)]
pub struct KlineOpt<'a> {
    /// The frequency of the kline data (e.g., "daily", "1min", "5min").
    pub freq: &'a str,
    /// The tier of the data, if applicable (e.g., Lead, SubLead for futures).
    pub tier: Option<Tier>,
    /// The adjustment type for the data, if any.
    pub adjust: Option<Adjust>,
    // /// Whether to use memory mapping when reading the data files.
    // pub memory_map: bool,
    /// Whether to concatenate tick dataframes when processing.
    pub concat_tick_df: bool,
}

impl Default for KlineOpt<'_> {
    fn default() -> Self {
        Self {
            freq: "daily",
            tier: None,
            adjust: None,
            // memory_map: true,
            concat_tick_df: false,
        }
    }
}

/// Configuration options for loading kline (candlestick) data.
impl<'a> KlineOpt<'a> {
    /// Sets the default tier for the given type if not already set.
    ///
    /// # Arguments
    ///
    /// * `typ` - The type of data (e.g., "future").
    ///
    /// # Returns
    ///
    /// Returns `Self` with the tier set if it was previously `None`.
    fn with_default_tier(mut self, typ: &str) -> Self {
        if self.tier.is_none() {
            let tier = match typ {
                "future" => Tier::Lead,
                _ => Tier::None,
            };
            self.tier = Some(tier);
        }
        self
    }

    /// Sets the default adjustment for the given type if not already set.
    ///
    /// # Arguments
    ///
    /// * `typ` - The type of data (e.g., "future").
    ///
    /// # Returns
    ///
    /// Returns `Self` with the adjustment set if it was previously `None`.
    fn with_default_adjust(mut self, typ: &str) -> Self {
        if self.adjust.is_none() {
            let adjust = match typ {
                "future" => {
                    if self.tier.is_none() {
                        self = self.with_default_adjust(typ);
                    }
                    let tier = self.tier.unwrap();
                    if tier != Tier::SubLead {
                        Adjust::Pre
                    } else {
                        Adjust::None
                    }
                },
                _ => Adjust::None,
            };
            self.adjust = Some(adjust);
        }
        self
    }

    /// Creates a `PathConfig` based on the current options and given type.
    ///
    /// # Arguments
    ///
    /// * `typ` - The type of data (e.g., "future").
    ///
    /// # Returns
    ///
    /// Returns a `PathConfig` with the appropriate settings.
    #[inline]
    pub fn path_config(&self, typ: &str) -> PathConfig {
        let opt = self.with_default_tier(typ).with_default_adjust(typ);
        PathConfig {
            config: CONFIG.path_finder.clone(),
            typ: typ.to_string(),
            freq: self.freq.into(),
            tier: opt.tier.unwrap(),
            adjust: opt.adjust.unwrap(),
        }
    }

    /// Creates a new `KlineOpt` with the given frequency.
    ///
    /// # Arguments
    ///
    /// * `freq` - The frequency of the kline data.
    ///
    /// # Returns
    ///
    /// Returns a new `KlineOpt` instance with the specified frequency.
    #[inline]
    pub fn new(freq: &'a str) -> Self {
        Self {
            freq,
            ..Default::default()
        }
    }

    /// Creates a new `KlineOpt` with the given frequency (alias for `new`).
    ///
    /// # Arguments
    ///
    /// * `freq` - The frequency of the kline data.
    ///
    /// # Returns
    ///
    /// Returns a new `KlineOpt` instance with the specified frequency.
    #[inline]
    pub fn freq(freq: &'a str) -> Self {
        Self {
            freq,
            ..Default::default()
        }
    }
}
