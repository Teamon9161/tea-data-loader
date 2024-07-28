use super::super::export::*;

#[derive(FactorBase, Default, Debug)]
pub struct Typ(pub Param);

impl PlFactor for Typ {
    #[inline]
    fn try_expr(&self) -> Result<Expr> {
        Ok(dsl::mean_horizontal([
            col("open"),
            col("high"),
            col("close"),
            col("low"),
        ])?)
    }
}

impl TFactor for Typ {
    #[inline]
    fn eval(&self, df: &DataFrame) -> Result<Series> {
        let mut expr = df.column("open")?.clone();
        for col in &["high", "close", "low"] {
            expr = (expr + df.column(col)?.clone())?;
        }
        Ok(expr / 4.)
    }
}

#[ctor::ctor]
fn register() {
    register_fac::<Typ>().unwrap()
}
