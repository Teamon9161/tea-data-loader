use polars::prelude::*;

use crate::export::*;

/// Represents the slope of the order book at higher levels (3 to 5).
///
/// This factor calculates the slope of the order book by comparing the volume
/// and price differences between levels 3 and 5 for both ask and bid sides.
///
/// # Interpretation
/// A larger slope at higher levels may indicate stronger conviction or information
/// advantage among more patient investors.
#[derive(FactorBase, FromParam, Default, Clone, Copy)]
pub struct ObSlopeHigh;

impl PlFactor for ObSlopeHigh {
    fn try_expr(&self) -> Result<Expr> {
        // 暂时将level3 -> level5定义为高档位, level1 -> level2为低档位
        let ask_slope_high = (ASK5 - ASK3) / (AskCumVol::fac(5) - AskCumVol::fac(3));
        let bid_slope_high = (BID5 - BID3) / (BidCumVol::fac(5) - BidCumVol::fac(3));
        ask_slope_high.imb(-bid_slope_high).try_expr()
    }
}

/// Represents the slope of the order book at lower levels (1 to 2).
///
/// This factor calculates the slope of the order book by comparing the volume
/// and price differences between levels 1 and 2 for both ask and bid sides.
///
/// # Interpretation
/// The slope at lower levels aligns with the elasticity logic of supply and demand,
/// where a larger slope on the buy side indicates less price sensitivity and potentially
/// higher expected returns.
#[derive(FactorBase, FromParam, Default, Clone)]
pub struct ObSlopeLow;

impl PlFactor for ObSlopeLow {
    fn try_expr(&self) -> Result<Expr> {
        // 暂时将level3 -> level5定义为高档位, level1 -> level2为低档位
        let ask_slope_low = (ASK2 - ASK1) / (AskCumVol::fac(2) - AskCumVol::fac(1));
        let bid_slope_low = (BID2 - BID1) / (BidCumVol::fac(2) - BidCumVol::fac(1));
        ask_slope_low.imb(-bid_slope_low).try_expr()
    }
}

/// 订单簿斜率凸性因子
///
/// 计算公式：`低档位斜率不平衡度 - 高档位斜率不平衡度`
///
/// 低档位斜率因子与上述的供需弹性逻辑相符，即买方低档斜率越大，投资者对于价格的敏感程度越小，股票预期收益越高。卖方低档斜
/// 率越大，弹性越小，预期收益越低。而高档位投资者往往耐心程度更强，且其更有可能拥
/// 有优势信息，会与低档位投资者产生相反的预测效果。如买方高档斜率越大，投资者对于
/// 更低的价格区间形成了较为一致的预期，股票的预期收益更低。反之，卖方高档斜率越大，
/// 投资者的心理预期价格较高，股票预期收益越高。
#[derive(FactorBase, FromParam, Default, Clone)]
pub struct ObSlopeConvex;

impl PlFactor for ObSlopeConvex {
    fn try_expr(&self) -> Result<Expr> {
        // 低档位斜率 - 高档位斜率
        (ObSlopeLow::fac(NONE) - ObSlopeHigh::fac(NONE)).try_expr()
    }
}

#[ctor::ctor]
fn register() {
    register_pl_fac::<ObSlopeHigh>().unwrap();
    register_pl_fac::<ObSlopeLow>().unwrap();
    register_pl_fac::<ObSlopeConvex>().unwrap();
}
