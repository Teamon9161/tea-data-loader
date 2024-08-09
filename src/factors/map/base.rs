use super::super::export::*;
use super::{Ret, Typ};

macro_rules! define_base_fac {
    ($($fac: ident),*) => {
        $(
            #[derive(FactorBase, Default, Debug, Clone)]
            pub struct $fac(pub Param);

            impl PlFactor for $fac {
                #[inline]
                fn try_expr(&self) -> Result<Expr> {
                    let fac_name = stringify!($fac).to_lowercase();
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
                    let fac_name = stringify!($fac).to_lowercase();
                    Ok(df.column(&fac_name)?.clone())
                }
            }

        )*

        #[ctor::ctor]
        fn register() {
            $(register_fac::<$fac>().unwrap());*
        }

    };
}

define_base_fac!(Open, High, Low, Close, Volume, Amt);

pub const OPEN: Open = Open(Param::None);
pub const HIGH: High = High(Param::None);
pub const LOW: Low = Low(Param::None);
pub const CLOSE: Close = Close(Param::None);
pub const VOLUME: Volume = Volume(Param::None);
pub const AMT: Amt = Amt(Param::None);
pub const TYP: Typ = Typ(Param::None);
pub const RET: Ret = Ret(Param::None);
