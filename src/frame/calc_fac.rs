use crate::prelude::*;

impl Frame {
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
}
