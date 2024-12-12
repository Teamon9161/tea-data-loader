macro_rules! define_base_fac {
    ($($fac:ident $(: $doc:expr)?),*) => {
        $(
            $(#[doc = $doc])?
            #[derive(FactorBase, FromParam, Default, Clone, Copy)]
            pub struct $fac;

            impl PlFactor for $fac {
                #[inline]
                fn try_expr(&self) -> Result<Expr> {
                    let fac_name = $crate::macros::to_snake_case(&stringify!($fac));
                    Ok(col(&fac_name))
                }
            }

            impl TryFrom<$fac> for Expr {
                type Error = anyhow::Error;

                fn try_from(value: $fac) -> Result<Self> {
                    value.try_expr()
                }
            }

            impl TFactor for $fac {
                #[inline]
                fn eval(&self, df: &DataFrame) -> Result<Series> {
                    let fac_name = $crate::macros::to_snake_case(&stringify!($fac));
                    Ok(df.column(&fac_name)?.as_materialized_series().clone())
                }
            }

            paste::paste! {

                pub const [<$fac:snake:upper>]: $crate::prelude::Factor<$fac> = $crate::prelude::Factor::<$fac>($fac);
            }
        )*

        #[ctor::ctor]
        fn register() {
            $(register_fac::<$fac>().unwrap());*
        }

    };
}
pub(super) use define_base_fac;

pub(super) fn to_snake_case(s: &str) -> String {
    let mut snake_case = String::new();

    for (i, c) in s.chars().enumerate() {
        if c.is_uppercase() {
            if i != 0 {
                snake_case.push('_');
            }
            snake_case.push(c.to_ascii_lowercase());
        } else {
            snake_case.push(c);
        }
    }

    snake_case
}
