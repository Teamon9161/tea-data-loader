use itertools::Itertools;

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
    pub fn with_pl_facs(mut self, facs: &[impl AsRef<dyn PlFactor>]) -> Result<Self> {
        let mut exprs = Vec::with_capacity(facs.len());
        let fac_names = facs.iter().map(|f| f.as_ref().name());
        let schema = self.schema()?;
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
}
