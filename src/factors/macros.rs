macro_rules! define_base_fac {
    ($($fac:ident $(: $doc:expr)?),*) => {
        $(
            $(#[doc = $doc])?
            #[derive(FactorBase, Default, Debug, Clone)]
            pub struct $fac(pub Param);

            impl PlFactor for $fac {
                #[inline]
                fn try_expr(&self) -> Result<Expr> {
                    let fac_name = $crate::factors::macros::to_snake_case(&stringify!($fac));
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
                    let fac_name = $crate::factors::macros::to_snake_case(&stringify!($fac));
                    Ok(df.column(&fac_name)?.clone())
                }
            }

            paste::paste! {
                pub const [<$fac:upper>]: $fac = $fac(Param::None);
                // pub const [<$fac:upper _E>]: ::std::sync::LazyLock<Expr> = ::std::sync::LazyLock::new(|| $fac(Param::None).expr());
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
