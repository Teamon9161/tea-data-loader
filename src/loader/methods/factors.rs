use std::sync::Arc;

use itertools::Itertools;
use polars::prelude::IntoLazy;
use polars::series::Series;
use rayon::prelude::*;
use tea_strategy::tevec::prelude::CollectTrustedToVec;

use crate::prelude::*;

impl DataLoader {
    /// Adds factors to the DataLoader using the specified backend.
    ///
    /// This method processes a list of factor names, parses them according to the chosen backend,
    /// and adds the resulting factors to each DataFrame in the DataLoader.
    ///
    /// # Arguments
    ///
    /// * `facs` - A slice of factor names to be added.
    /// * `backend` - The backend to use for factor calculation (Polars or Tevec).
    ///
    /// # Returns
    ///
    /// A `Result` containing the modified `DataLoader` with new factors added, or an error.
    #[inline]
    pub fn with_facs<F: AsRef<str>>(mut self, facs: &[F], backend: Backend) -> Result<Self> {
        use crate::factors::parse_pl_fac;
        let facs = facs.iter().map(|v| v.as_ref());
        let len = facs.len();
        let schema = self.schema()?;
        let filtered_facs = facs.filter(|f| (!schema.contains(f)) && !f.is_empty());
        match backend {
            Backend::Polars => {
                let mut pl_facs = Vec::with_capacity(len);
                let mut t_facs = Vec::new();
                for f in filtered_facs {
                    if let Ok(fac) = parse_pl_fac(f) {
                        pl_facs.push(fac);
                    } else {
                        let fac = parse_t_fac(f)?;
                        t_facs.push(fac);
                    }
                }
                if t_facs.is_empty() {
                    self.with_pl_facs(&pl_facs)
                } else {
                    self.with_pl_facs(&pl_facs)?.with_t_facs(&t_facs)
                }
            },
            Backend::Tevec => {
                let mut pl_facs = Vec::new();
                let mut t_facs = Vec::with_capacity(len);
                for f in filtered_facs {
                    if let Ok(fac) = parse_t_fac(f) {
                        t_facs.push(fac);
                    } else {
                        let fac = parse_pl_fac(f)?;
                        pl_facs.push(fac);
                    }
                }
                if pl_facs.is_empty() {
                    self.with_t_facs(&t_facs)
                } else {
                    self.with_t_facs(&t_facs)?.with_pl_facs(&pl_facs)
                }
            },
        }
    }

    /// Adds Polars factors to the DataLoader.
    ///
    /// This method processes a slice of Polars factors and adds them to each DataFrame
    /// in the DataLoader.
    ///
    /// # Arguments
    ///
    /// * `facs` - A slice of Polars factors to be added.
    ///
    /// # Returns
    ///
    /// A `Result` containing the modified `DataLoader` with new Polars factors added, or an error.
    #[inline]
    pub fn with_pl_facs<F: AsRef<dyn PlFactor>>(mut self, facs: &[F]) -> Result<Self> {
        let schema = self.schema()?;
        let mut exprs = Vec::with_capacity(facs.len());
        let fac_names = facs.iter().map(|f| f.as_ref().name());
        facs.iter()
            .zip(fac_names)
            .filter(|(_, n)| (!schema.contains(n)) && !n.is_empty())
            .unique_by(|(_, n)| n.clone())
            .try_for_each::<_, Result<()>>(|(f, n)| {
                let expr = f.as_ref().try_expr()?.alias(&n);
                exprs.push(expr);
                Ok(())
            })?;
        self.with_columns(exprs)
    }

    /// Adds a single Polars factor to the DataLoader.
    ///
    /// This method processes a single Polars factor and adds it to each DataFrame
    /// in the DataLoader.
    ///
    /// # Arguments
    ///
    /// * `fac` - A Polars factor to be added.
    ///
    /// # Returns
    ///
    /// A `Result` containing the modified `DataLoader` with the new Polars factor added, or an error.
    #[inline]
    pub fn with_pl_fac<F: PlFactor>(self, fac: F) -> Result<Self> {
        self.with_column(fac.try_expr()?.alias(&fac.name()))
    }

    /// Adds Tfactors to the DataLoader.
    ///
    /// This method processes a slice of Tfactors and adds them to each DataFrame
    /// in the DataLoader.
    ///
    /// # Arguments
    ///
    /// * `facs` - A slice of Tfactors to be added.
    ///
    /// # Returns
    ///
    /// A `Result` containing the modified `DataLoader` with new Tfactors added, or an error.
    #[inline]
    pub fn with_t_facs<F: AsRef<dyn TFactor>>(self, facs: &[F]) -> Result<Self> {
        let mut out = self.collect(true)?;
        let schema = out.schema()?;
        let facs = facs
            .iter()
            .map(|f| f.as_ref())
            .filter(|f| (!schema.contains(&f.name())) && (f.name() != ""))
            .unique_by(|f| f.name())
            .collect_vec();
        let fac_names = facs.iter().map(|f| f.name()).collect_trusted_to_vec();
        let dfs: Vec<Frame> = crate::POOL.install(|| {
            out.dfs
                .0
                .into_par_iter()
                .map(|df| {
                    let mut df = df.unwrap_eager();
                    let series_vec: Vec<Series> = facs
                        .par_iter()
                        .zip(&fac_names)
                        .map(|(fac, name)| fac.eval(&df).unwrap().with_name(name))
                        .collect();
                    df.hstack_mut(&series_vec).unwrap();
                    df.lazy().into()
                })
                .collect()
        });
        out.dfs = dfs.into();
        Ok(out)
    }

    /// Adds a single Tfactor to the DataLoader.
    ///
    /// This method processes a single Tfactor and adds it to each DataFrame
    /// in the DataLoader.
    ///
    /// # Arguments
    ///
    /// * `fac` - A tfactor to be added.
    ///
    /// # Returns
    ///
    /// A `Result` containing the modified `DataLoader` with the new Tfactor added, or an error.
    #[inline]
    pub fn with_t_fac<F: TFactor>(self, fac: F) -> Result<Self> {
        let facs: Vec<Arc<dyn TFactor>> = vec![Arc::new(fac)];
        self.with_t_facs(&facs)
    }
}
