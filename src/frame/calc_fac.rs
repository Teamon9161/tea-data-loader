use crate::prelude::*;

impl Frame {
    /// Adds new columns to the frame based on the provided Polars factors.
    ///
    /// This method takes a slice of Polars factors, converts each factor to a Polars expression,
    /// and adds the resulting expressions as new columns to the frame.
    ///
    /// # Arguments
    ///
    /// * `facs` - A slice of types that can be referenced as `PlFactor` trait objects.
    ///
    /// # Returns
    ///
    /// * `Result<Self>` - A new `Frame` with the added columns if successful, or an error if any factor conversion fails.
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
