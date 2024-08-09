#![feature(iterator_try_collect)]

mod configs;
mod enums;
mod frame;
mod loader;
mod path_finder;
mod polars_ext;

pub(crate) use tea_strategy::tevec;
pub mod factors;
pub mod prelude;
pub mod strategy;
#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::prelude::*;
    use crate::factors::map::Typ;
    #[test]
    pub fn test_base() -> Result<()> {
        let facs: Vec<Arc<dyn PlFactor>> = vec![Arc::new(Typ::default())];
        let dl = DataLoader::new("future")
            .with_symbols(["A", "CU", "RB"])
            .with_start("2020-01-01")
            .kline("min", None, None, true)?
            .with_noadj(None, false, true)?
            .with_pl_facs(&facs)?
            .with_facs(&["typ_1"], Backend::Polars)?
            .collect(true)?;
        dbg!("{:#?}", &dl["A"]);
        let dl = DataLoader::new("future")
            .kline("daily", None, None, true)?
            .with_noadj(None, false, true)?
            .collect(true)?;
        dbg!("{:#?}", &dl["AG"]);
        Ok(())
    }
}
