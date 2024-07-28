use std::sync::Arc;

use polars::series::Series;
use rayon::prelude::*;
use tea_strategy::tevec::prelude::CollectTrustedToVec;

use crate::prelude::*;

impl DataLoader {
    #[inline]
    pub fn with_facs<'a, F: AsRef<[&'a str]>>(mut self, facs: F, backend: Backend) -> Result<Self> {
        use crate::factors::parse_pl_fac;
        let schema = self.schema()?;
        let facs = facs.as_ref().into_iter().filter(|n| !schema.contains(n));
        match backend {
            Backend::Polars => {
                let facs = facs.map(|f| parse_pl_fac(f)).try_collect::<Vec<_>>()?;
                self.with_pl_facs(&facs)
            },
            Backend::Tevec => todo!(),
        }
    }

    #[inline]
    pub fn with_pl_facs<F: AsRef<dyn PlFactor>>(self, facs: &[F]) -> Result<Self> {
        let mut exprs = Vec::with_capacity(facs.len());
        for f in facs {
            let f = f.as_ref();
            let expr = f.try_expr()?.alias(&f.name());
            exprs.push(expr);
        }
        self.with_columns(exprs)
    }

    #[inline]
    pub fn with_pl_fac<F: PlFactor>(self, fac: F) -> Result<Self> {
        self.with_column(fac.try_expr()?.alias(&fac.name()))
    }

    #[inline]
    pub fn with_tp_facs<F: AsRef<dyn TFactor>>(self, facs: &[F]) -> Result<Self> {
        let mut out = self.collect(true)?;
        let facs = facs
            .into_iter()
            .map(|f| f.as_ref())
            .collect_trusted_to_vec();
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
    pub fn with_tp_fac<F: TFactor>(self, fac: F) -> Result<Self> {
        let facs: Vec<Arc<dyn TFactor>> = vec![Arc::new(fac)];
        self.with_tp_facs(&facs)
    }
}
