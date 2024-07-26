use crate::prelude::*;

impl Frame {
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
}
