use polars::prelude::IntoLazy;
use polars::series::Series;
use rayon::prelude::*;
use tea_strategy::tevec::prelude::CollectTrustedToVec;

use crate::prelude::*;

impl DataLoader {
    pub fn with_strategies<S: AsRef<str>>(mut self, strategies: &[S]) -> Result<Self> {
        let schema = self.schema()?;
        let strategies = strategies.iter().filter_map(|n| {
            let n = n.as_ref();
            if !schema.contains(n) {
                Some(n)
            } else {
                None
            }
        });
        let works = strategies
            .map(|s| s.parse())
            .try_collect::<Vec<StrategyWork>>()?;
        // calculate factors
        let facs = works
            .iter()
            .map(|w| w.fac.as_ref())
            .collect_trusted_to_vec();
        let mut dl = self.with_facs(&facs, Default::default())?;
        // calculate strategies
        let frames: Vec<Frame> = dl
            .dfs
            .0
            .into_par_iter()
            .map(|f| {
                let mut df = f.collect().unwrap();
                let series = works
                    .par_iter()
                    .map(|w| {
                        let mut res = w.eval(&df).unwrap();
                        if res.name() == "" {
                            res.rename(w.name.as_ref().unwrap());
                            res
                        } else {
                            res
                        }
                    })
                    .collect::<Vec<Series>>();
                df.hstack_mut(&series).unwrap();
                df.lazy().into()
            })
            .collect();
        dl.dfs = frames.into();
        Ok(dl)
    }
}
