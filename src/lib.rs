#![feature(iterator_try_collect)]

mod configs;
mod enums;
mod frame;
mod loader;
mod path_finder;

pub mod prelude;

#[cfg(test)]
mod tests {
    use super::prelude::*;
    #[test]
    pub fn test_base() -> Result<()> {
        let dl = DataLoader::new("future")
            .with_symbols(["A", "CU", "RB"])
            .with_start("2020-01-01")
            .kline("min", None, None, true)?
            .with_noadj(None, false, true)?
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
