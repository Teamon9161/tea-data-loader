use std::collections::HashMap;
use std::sync::Arc;

use polars::prelude::LazyFrame;

use crate::path_finder::{PathConfig, PathFinder};
use crate::prelude::*;

impl DataLoader {
    pub fn with_multiplier(mut self) -> Result<Self> {
        if self.multiplier.is_some() {
            return Ok(self);
        }
        match self.typ.as_ref() {
            "future" => {
                let path_config = PathConfig {
                    config: CONFIG.path_finder.clone(),
                    typ: "future".to_string(),
                    freq: "info".to_string(),
                    tier: Tier::None,
                    adjust: Adjust::None,
                };
                let finder = PathFinder::new(path_config)?;
                let path = finder.path()?;
                let df = LazyFrame::scan_ipc(path, Default::default())?.collect()?;
                let symbol = df.column("underlying_symbol")?.str()?;
                let multiplier = df.column("contract_multiplier")?.f64()?;
                let map: HashMap<Arc<str>, f64> = symbol
                    .into_iter()
                    .zip(multiplier)
                    .map(|(s, m)| (s.unwrap().into(), m.unwrap_or(f64::NAN)))
                    .collect();
                self.multiplier = Some(map);
            },
            _ => {
                eprintln!("unsupported type in multiplier: {}", self.typ);
                self.multiplier = Some(HashMap::with_capacity(0));
            },
        }
        Ok(self)
    }
}
