use polars::series::Series;
use rayon::prelude::*;
use tevec::prelude::CollectTrustedToVec;

use crate::prelude::*;

impl DataLoader {
    #[inline]
    pub fn with_pl_facs<'a, F: AsRef<[&'a dyn PlFactor]>>(self, facs: F) -> Result<Self> {
        let facs = facs.as_ref();
        let mut exprs = Vec::with_capacity(facs.len());
        for f in facs {
            let expr = f.try_expr()?.alias(&f.name());
            exprs.push(expr);
        }
        self.with_columns(exprs)
    }

    #[inline]
    pub fn with_pl_fac<'a, F: PlFactor>(self, fac: F) -> Result<Self> {
        self.with_column(fac.try_expr()?.alias(&fac.name()))
    }

    #[inline]
    pub fn with_tp_facs<'a, F: AsRef<[&'a dyn TFactor]>>(self, facs: F) -> Result<Self> {
        let mut out = self.collect(true)?;
        let facs = facs.as_ref();
        let fac_names = facs.iter().map(|f| f.name()).collect_trusted_to_vec();
        let dfs: Vec<Frame> = out
            .dfs
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
                df.into()
            })
            .collect();
        out.dfs = dfs.into();
        Ok(out)
    }

    #[inline]
    pub fn with_tp_fac<'a, F: TFactor>(self, fac: F) -> Result<Self> {
        self.with_tp_facs([&fac as &dyn TFactor])
    }
}
