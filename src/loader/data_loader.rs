use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Result;
use polars::prelude::{NamedFrom, SchemaRef};
use polars::series::Series;
use tea_strategy::tevec::dtype::Cast;
use tea_strategy::tevec::prelude::DateTime;

use crate::prelude::{Frame, Frames};

#[derive(Clone)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct DataLoader {
    pub typ: Arc<str>,
    pub dfs: Frames,
    pub symbols: Option<Vec<Arc<str>>>,
    pub freq: Option<Arc<str>>,
    pub start: Option<DateTime>,
    pub end: Option<DateTime>,
    pub kline_path: Option<PathBuf>,
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
    #[inline]
    pub fn new(typ: &str) -> Self {
        DataLoader {
            typ: typ.into(),
            ..Default::default()
        }
    }

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

    #[inline]
    pub fn new_from_dfs<F: Into<Frames>>(dfs: F) -> Self {
        DataLoader {
            dfs: dfs.into(),
            ..Default::default()
        }
    }

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

    #[inline]
    pub fn len(&self) -> usize {
        self.dfs.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.dfs.is_empty()
    }

    #[inline]
    pub fn with_start<DT: Cast<DateTime>>(mut self, start: DT) -> Self {
        self.start = Some(start.cast());
        self
    }

    #[inline]
    pub fn with_end<DT: Cast<DateTime>>(mut self, end: DT) -> Self {
        self.end = Some(end.cast());
        self
    }

    #[inline]
    pub fn with_symbols<S: IntoIterator<Item = A>, A: Into<Arc<str>>>(
        mut self,
        symbols: S,
    ) -> Self {
        self.symbols = Some(symbols.into_iter().map(Into::into).collect());
        self
    }

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

    #[inline]
    pub fn collect(mut self, par: bool) -> Result<Self> {
        self.dfs = self.dfs.collect(par)?;
        Ok(self)
    }

    #[inline]
    pub fn lazy(mut self) -> Self {
        self.dfs = self.dfs.lazy();
        self
    }

    #[inline]
    pub fn with_dfs<F: Into<Frames>>(mut self, dfs: F) -> Self {
        self.dfs = dfs.into();
        self
    }

    #[inline]
    pub fn with_type(mut self, typ: &str) -> Self {
        self.typ = typ.into();
        self
    }

    #[inline]
    pub fn with_freq(mut self, freq: &str) -> Self {
        self.freq = Some(freq.into());
        self
    }

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

    #[inline]
    pub fn empty_copy(&self) -> Self {
        let dfs: Vec<Frame> = Vec::with_capacity(self.len());
        self.copy_with_dfs(dfs)
    }

    #[inline]
    pub fn daily_col(&self) -> &str {
        if let Some(freq) = self.freq.as_deref() {
            if freq == "daily" {
                return "time";
            }
        }
        "trading_date"
    }

    #[inline]
    pub fn contains(&mut self, name: &str) -> bool {
        if self.dfs.is_empty() {
            false
        } else {
            self[0].schema().unwrap().contains(name)
        }
    }

    #[inline]
    pub fn schema(&mut self) -> Result<SchemaRef> {
        if self.is_empty() {
            Ok(SchemaRef::default())
        } else {
            self[0].schema()
        }
    }
}
