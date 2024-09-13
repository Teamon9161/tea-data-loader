#![feature(iterator_try_collect)]

mod configs;
mod enums;
#[cfg(feature = "fac-analyse")]
mod fac_analyse;
mod frame;
mod loader;
mod path_finder;
mod polars_ext;

pub mod export;
pub mod factors;
pub mod prelude;
pub mod strategy;

use std::sync::LazyLock;

#[cfg(feature = "fac-analyse")]
pub use fac_analyse::linspace;
pub use factor_macro as macros;
pub use loader::utils;
use rayon::{ThreadPool, ThreadPoolBuilder};

pub static POOL: LazyLock<ThreadPool> = LazyLock::new(|| {
    let thread_name = "tea_dataloader";
    ThreadPoolBuilder::new()
        .num_threads(
            std::env::var("TDL_NUM_THREADS")
                .map(|s| s.parse::<usize>().expect("TDL_NUM_THREADS should be int"))
                .unwrap_or_else(|_| {
                    let n = std::thread::available_parallelism()
                        .unwrap_or_else(|_| std::num::NonZeroUsize::new(1).unwrap())
                        .get();
                    if n >= 4 {
                        n - 1
                    } else {
                        n
                    }
                }),
        )
        .thread_name(move |i| format!("{}-{}", thread_name, i))
        .build()
        .expect("could not spawn threads")
});

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
            .kline(KlineOpt::freq("min"))?
            .with_noadj(None, false, true)?
            .with_pl_facs(&facs)?
            .with_facs(&["typ_1"], Backend::Polars)?
            .collect(true)?;
        dbg!("{:#?}", &dl["A"]);
        let dl = DataLoader::new("future")
            .kline(KlineOpt::freq("daily"))?
            .with_noadj(None, false, true)?
            .collect(true)?;
        dbg!("{:#?}", &dl["AG"]);
        Ok(())
    }
}
