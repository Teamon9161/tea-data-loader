use super::super::export::*;
use super::{Ret, Typ};

macro_rules! define_base_fac {
    ($($fac:ident, $doc:expr),*) => {
        $(
            #[doc = $doc]
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

define_base_fac!(
    Open,
    "开盘价，代表每个交易周期的起始价格。",
    High,
    "最高价，代表每个交易周期内的最高交易价格。",
    Low,
    "最低价，代表每个交易周期内的最低交易价格。",
    Close,
    "收盘价，代表每个交易周期的结束价格。",
    Volume,
    "成交量，代表每个交易周期内的交易数量。",
    Amt,
    "成交额，代表每个交易周期内的交易金额。"
);

/// 开盘价
pub const OPEN: Open = Open(Param::None);

/// 最高价
pub const HIGH: High = High(Param::None);

/// 最低价
pub const LOW: Low = Low(Param::None);

/// 收盘价
pub const CLOSE: Close = Close(Param::None);

/// 成交量
pub const VOLUME: Volume = Volume(Param::None);

/// 成交额
pub const AMT: Amt = Amt(Param::None);

/// 典型价格
pub const TYP: Typ = Typ(Param::None);

/// 收益率
pub const RET: Ret = Ret(Param::None);
