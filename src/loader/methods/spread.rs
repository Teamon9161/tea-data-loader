use polars::prelude::*;

use crate::path_finder::*;
use crate::prelude::*;

impl DataLoader {
    pub fn with_spread(mut self, flag: bool) -> Result<Self> {
        if !flag || self.contains("twap_spread") {
            return Ok(self);
        }
        let path_config = PathConfig {
            config: CONFIG.path_finder.clone(),
            freq: "tick_spread".into(),
            typ: self.typ.to_string(),
            tier: Tier::Lead,
            adjust: Adjust::None,
        };
        let spread_path = PathFinder::new(path_config)?.path()?;
        if self.freq.as_deref().unwrap() != "min" {
            unimplemented!("Only support join spread for minute data for now")
        }
        self.left_join(spread_path, [col("time")], [col("datetime")], flag)
    }
}
