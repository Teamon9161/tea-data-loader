use polars::prelude::IntoLazy;
use polars::series::Series;
use rayon::prelude::*;
use tea_strategy::tevec::prelude::CollectTrustedToVec;

use crate::prelude::*;

impl DataLoader {
    /// Adds strategies to the DataLoader.
    ///
    /// This method applies a list of strategies to the data in the DataLoader,
    /// calculating new columns based on the provided strategy definitions.
    ///
    /// # Arguments
    ///
    /// * `strategies` - A slice of strings, each representing a strategy to be applied.
    ///
    /// # Returns
    ///
    /// A `Result` containing the modified `DataLoader` if successful, or an error if the operation fails.
    ///
    /// # Behavior
    ///
    /// - Filters out strategies that already exist in the schema.
    /// - Parses the strategies into `StrategyWork` objects.
    /// - Calculates necessary factors for the strategies.
    /// - Applies the strategies in parallel to each DataFrame in the DataLoader.
    /// - Adds the resulting series as new columns to the DataFrames.
    ///
    /// # Errors
    ///
    /// This method can return an error if:
    /// - There's an issue parsing the strategies.
    /// - There's a problem calculating factors or applying strategies.
    /// - Any other data processing error occurs.
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
        let dl = self.with_facs(&facs, Default::default())?.par_apply(|f| {
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
            df.lazy()
        });
        Ok(dl)
    }
}
