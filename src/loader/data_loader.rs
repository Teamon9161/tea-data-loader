use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Result;
use polars::prelude::{NamedFrom, SchemaRef};
use polars::series::Series;
use tea_strategy::tevec::dtype::Cast;
use tea_strategy::tevec::prelude::DateTime;

use crate::prelude::{Frame, Frames};

/// A struct representing a data loader for financial time series data.
///
/// This struct holds various pieces of information related to the loaded data,
/// including the data frames, symbols, time range, and other metadata.
#[derive(Clone)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct DataLoader {
    /// The type of data, such as future, bond, stock, etc.
    pub typ: Arc<str>,
    /// The collection of data frames.
    pub dfs: Frames,
    /// An optional vector of symbols associated with the data.
    pub symbols: Option<Vec<Arc<str>>>,
    /// An optional frequency of the data (e.g., "1d" for daily, "1h" for hourly).
    pub freq: Option<Arc<str>>,
    /// The optional start date/time of the data range.
    pub start: Option<DateTime>,
    /// The optional end date/time of the data range.
    pub end: Option<DateTime>,
    /// An optional path to the kline data files.
    pub kline_path: Option<PathBuf>,
    /// An optional hashmap of multipliers for each symbol.
    pub multiplier: Option<HashMap<Arc<str>, f64>>,
}

impl Default for DataLoader {
    #[inline]
    fn default() -> Self {
        DataLoader {
            typ: "".into(),
            dfs: Default::default(),
            symbols: None,
            freq: None,
            start: None,
            end: None,
            kline_path: None,
            multiplier: None,
        }
    }
}

impl DataLoader {
    /// Creates a new `DataLoader` instance with the specified type.
    ///
    /// # Arguments
    ///
    /// * `typ` - A string slice that holds the type of data (e.g., "future", "bond", "stock").
    ///
    /// # Returns
    ///
    /// A new `DataLoader` instance with the specified type and default values for other fields.
    #[inline]
    pub fn new(typ: &str) -> Self {
        DataLoader {
            typ: typ.into(),
            ..Default::default()
        }
    }

    /// Creates a new `DataLoader` instance with the specified type and symbols.
    ///
    /// # Arguments
    ///
    /// * `typ` - A string slice that holds the type of data.
    /// * `symbols` - An iterable of symbols that can be converted into `Arc<str>`.
    ///
    /// # Returns
    ///
    /// A new `DataLoader` instance with the specified type and symbols, and default values for other fields.
    #[inline]
    pub fn new_with_symbols<S: IntoIterator<Item = A>, A: Into<Arc<str>>>(
        typ: &str,
        symbols: S,
    ) -> Self {
        DataLoader {
            typ: typ.into(),
            symbols: Some(symbols.into_iter().map(Into::into).collect()),
            ..Default::default()
        }
    }

    /// Creates a new `DataLoader` instance from the provided data frames.
    ///
    /// # Arguments
    ///
    /// * `dfs` - Data frames that can be converted into `Frames`.
    ///
    /// # Returns
    ///
    /// A new `DataLoader` instance with the provided data frames and default values for other fields.
    #[inline]
    pub fn new_from_dfs<F: Into<Frames>>(dfs: F) -> Self {
        DataLoader {
            dfs: dfs.into(),
            ..Default::default()
        }
    }

    /// Creates a new `DataLoader` instance from the provided symbols and data frames.
    ///
    /// # Arguments
    ///
    /// * `symbols` - An iterable of symbols that can be converted into `Arc<str>`.
    /// * `dfs` - Data frames that can be converted into `Frames`.
    ///
    /// # Returns
    ///
    /// A new `DataLoader` instance with the provided symbols and data frames, and default values for other fields.
    #[inline]
    pub fn new_from_symbol_dfs<F: Into<Frames>, S: IntoIterator<Item = A>, A: Into<Arc<str>>>(
        symbols: S,
        dfs: F,
    ) -> Self {
        DataLoader {
            dfs: dfs.into(),
            symbols: Some(symbols.into_iter().map(Into::into).collect()),
            ..Default::default()
        }
    }

    /// Returns the number of data frames in the `DataLoader`.
    #[inline]
    pub fn len(&self) -> usize {
        self.dfs.len()
    }

    /// Returns `true` if the `DataLoader` contains no data frames.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.dfs.is_empty()
    }

    /// Checks if the `DataLoader` is lazy.
    ///
    /// This method determines if the data loading is lazy by checking the first data frame.
    /// If the `DataLoader` is empty, it is considered not lazy.
    ///
    /// # Returns
    ///
    /// `true` if the `DataLoader` is lazy, `false` otherwise.
    #[inline]
    pub fn is_lazy(&self) -> bool {
        if self.is_empty() {
            false
        } else {
            self.dfs[0].is_lazy()
        }
    }

    /// Checks if the `DataLoader` is eager.
    ///
    /// This method is the opposite of `is_lazy()`. It returns `true` if the `DataLoader`
    /// is not lazy, which means it's eager.
    ///
    /// # Returns
    ///
    /// `true` if the `DataLoader` is eager, `false` otherwise.
    #[inline]
    pub fn is_eager(&self) -> bool {
        !self.is_lazy()
    }

    /// Sets the start date/time for the `DataLoader`.
    ///
    /// # Arguments
    ///
    /// * `start` - A value that can be cast to `DateTime`.
    ///
    /// # Returns
    ///
    /// The modified `DataLoader` instance.
    #[inline]
    pub fn with_start<DT: Cast<DateTime>>(mut self, start: DT) -> Self {
        self.start = Some(start.cast());
        self
    }

    /// Sets the end date/time for the `DataLoader`.
    ///
    /// # Arguments
    ///
    /// * `end` - A value that can be cast to `DateTime`.
    ///
    /// # Returns
    ///
    /// The modified `DataLoader` instance.
    #[inline]
    pub fn with_end<DT: Cast<DateTime>>(mut self, end: DT) -> Self {
        self.end = Some(end.cast());
        self
    }

    /// Sets the symbols for the `DataLoader`.
    ///
    /// # Arguments
    ///
    /// * `symbols` - An iterable of symbols that can be converted into `Arc<str>`.
    ///
    /// # Returns
    ///
    /// The modified `DataLoader` instance.
    #[inline]
    pub fn with_symbols<S: IntoIterator<Item = A>, A: Into<Arc<str>>>(
        mut self,
        symbols: S,
    ) -> Self {
        self.symbols = Some(symbols.into_iter().map(Into::into).collect());
        self
    }

    /// Returns a vector of symbol references if symbols are present in the `DataLoader`.
    ///
    /// # Returns
    ///
    /// An `Option<Vec<&str>>` containing references to the symbols if they exist,
    /// or `None` if no symbols are present in the `DataLoader`.
    #[inline]
    pub fn get_symbols(&self) -> Option<Vec<&str>> {
        self.symbols
            .as_ref()
            .map(|symbols| symbols.iter().map(|s| s.as_ref()).collect())
    }

    /// Returns a `Series` containing the symbols in the `DataLoader`.
    ///
    /// If no symbols are present, returns an empty `Series` with the name "symbol".
    #[inline]
    pub fn get_symbol_series(&self) -> Series {
        self.symbols
            .as_ref()
            .map(|symbols| {
                symbols
                    .iter()
                    .map(|s| s.as_ref())
                    .collect::<Series>()
                    .with_name("symbol")
            })
            .unwrap_or_else(|| Series::new("symbol", &vec![None::<&str>; 0]))
    }

    /// Collects the data frames in the `DataLoader`.
    ///
    /// # Arguments
    ///
    /// * `par` - A boolean indicating whether to use parallel processing.
    ///
    /// # Returns
    ///
    /// A `Result` containing the modified `DataLoader` instance or an error.
    #[inline]
    pub fn collect(mut self, par: bool) -> Result<Self> {
        self.dfs = self.dfs.collect(par)?;
        Ok(self)
    }

    /// Converts the data frames in the `DataLoader` to lazy frames.
    ///
    /// # Returns
    ///
    /// The modified `DataLoader` instance with lazy frames.
    #[inline]
    pub fn lazy(mut self) -> Self {
        self.dfs = self.dfs.lazy();
        self
    }

    /// Sets the data frames for the `DataLoader`.
    ///
    /// # Arguments
    ///
    /// * `dfs` - Data frames that can be converted into `Frames`.
    ///
    /// # Returns
    ///
    /// The modified `DataLoader` instance.
    #[inline]
    pub fn with_dfs<F: Into<Frames>>(mut self, dfs: F) -> Self {
        self.dfs = dfs.into();
        self
    }

    /// Sets the type for the `DataLoader`.
    ///
    /// # Arguments
    ///
    /// * `typ` - Any type that can be referenced as a str.
    ///
    /// # Returns
    ///
    /// The modified `DataLoader` instance.
    #[inline]
    pub fn with_type<R: AsRef<str>>(mut self, typ: R) -> Self {
        self.typ = typ.as_ref().into();
        self
    }

    /// Sets the frequency for the `DataLoader`.
    ///
    /// # Arguments
    ///
    /// * `freq` - Any type that can be referenced as a str.
    ///
    /// # Returns
    ///
    /// The modified `DataLoader` instance.
    #[inline]
    pub fn with_freq<R: AsRef<str>>(mut self, freq: R) -> Self {
        self.freq = Some(freq.as_ref().into());
        self
    }

    /// Creates a copy of the `DataLoader` with new data frames.
    ///
    /// # Arguments
    ///
    /// * `dfs` - Data frames that can be converted into `Frames`.
    ///
    /// # Returns
    ///
    /// A new `DataLoader` instance with the same metadata as the current instance but with new data frames.
    #[inline]
    pub fn copy_with_dfs<F: Into<Frames>>(&self, dfs: F) -> Self {
        DataLoader {
            typ: self.typ.clone(),
            dfs: dfs.into(),
            symbols: self.symbols.clone(),
            freq: self.freq.clone(),
            start: self.start,
            end: self.end,
            kline_path: self.kline_path.clone(),
            multiplier: self.multiplier.clone(),
        }
    }

    /// Creates an empty copy of the `DataLoader`.
    ///
    /// # Returns
    ///
    /// A new `DataLoader` instance with the same metadata as the current instance but with empty data frames.
    #[inline]
    pub fn empty_copy(&self) -> Self {
        let dfs: Vec<Frame> = Vec::with_capacity(self.len());
        self.copy_with_dfs(dfs)
    }

    /// Returns the name of the column representing the daily time or trading date.
    ///
    /// # Returns
    ///
    /// A string slice containing the name of the column ("time" for daily frequency, "trading_date" otherwise).
    #[inline]
    pub fn daily_col(&self) -> &str {
        if let Some(freq) = self.freq.as_deref() {
            if freq == "daily" {
                return "time";
            }
        }
        "trading_date"
    }

    /// Checks if the `DataLoader` contains a column with the given name.
    ///
    /// # Arguments
    ///
    /// * `name` - A string slice containing the name of the column to check.
    ///
    /// # Returns
    ///
    /// A boolean indicating whether the column exists in the first data frame.
    #[inline]
    pub fn contains(&mut self, name: &str) -> bool {
        if self.dfs.is_empty() {
            false
        } else {
            self[0].schema().unwrap().contains(name)
        }
    }

    /// Returns the schema of the first data frame in the `DataLoader`.
    ///
    /// # Returns
    ///
    /// A `Result` containing the `SchemaRef` of the first data frame or an error.
    /// If the `DataLoader` is empty, returns an empty `SchemaRef`.
    #[inline]
    pub fn schema(&self) -> Result<SchemaRef> {
        if self.is_empty() {
            Ok(SchemaRef::default())
        } else {
            self[0].clone().schema()
        }
    }
}
