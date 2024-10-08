use super::super::export::*;

/// 价格和成交量相关系数（Price-Volume Correlation）
///
/// 这个因子计算价格（收盘价）和成交量之间的滚动相关系数。
///
/// 计算公式：
/// PVCorr = Rolling Correlation(Close, Volume, N)
///
/// 其中：
/// - Close: 收盘价
/// - Volume: 成交量
/// - N: 滚动窗口大小，由 Param 参数指定
///
/// 指标解读：
/// - 正相关：价格上涨时成交量增加，下跌时成交量减少，可能表示趋势较强
/// - 负相关：价格上涨时成交量减少，下跌时成交量增加，可能表示市场存在分歧
/// - 接近零：价格和成交量之间没有明显的相关性
#[derive(FactorBase, FromParam, Default, Clone, Copy)]
pub struct PVCorr(pub usize);

impl PlFactor for PVCorr {
    #[inline]
    fn try_expr(&self) -> Result<Expr> {
        CLOSE.corr(VOLUME, self.0).try_expr()
    }
}

/// 收益率和成交量变动相关系数（Price Return - Volume Return Correlation）
///
/// 这个因子计算收益率（价格变动百分比）和成交量变动百分比之间的滚动相关系数。
///
/// 计算公式：
/// PrVrCorr = Rolling Correlation(Price Return, Volume Return, N)
///
/// 其中：
/// - Price Return: 收盘价的百分比变化
/// - Volume Return: 成交量的百分比变化
/// - N: 滚动窗口大小，由 Param 参数指定
#[derive(FactorBase, FromParam, Default, Clone, Copy)]
pub struct PrVrCorr(pub usize);

impl PlFactor for PrVrCorr {
    #[inline]
    fn try_expr(&self) -> Result<Expr> {
        CLOSE.pct(1).corr(VOLUME.pct(1), self.0).try_expr()
    }
}

/// 收益率和成交量的相关系数（Price Return - Volume Correlation）
///
/// 这个因子计算收益率（价格变动百分比）和成交量之间的滚动相关系数。
///
/// 计算公式：
/// PrVCorr = Rolling Correlation(Price Return, Volume, N)
///
/// 其中：
/// - Price Return: 收盘价的百分比变化
/// - Volume: 成交量
/// - N: 滚动窗口大小，由 Param 参数指定
#[derive(FactorBase, FromParam, Default, Clone, Copy)]
pub struct PrVCorr(pub usize);

impl PlFactor for PrVCorr {
    #[inline]
    fn try_expr(&self) -> Result<Expr> {
        CLOSE.pct(1).corr(VOLUME, self.0).try_expr()
    }
}

/// 价格和成交量变动的相关系数（Price - Volume Return Correlation）
///
/// 这个因子计算价格和成交量变动百分比之间的滚动相关系数。
///
/// 计算公式：
/// PVrCorr = Rolling Correlation(Price, Volume Return, N)
///
/// 其中：
/// - Price: 收盘价
/// - Volume Return: 成交量的百分比变化
/// - N: 滚动窗口大小，由 Param 参数指定
#[derive(FactorBase, FromParam, Default, Clone, Copy)]
pub struct PVrCorr(pub usize);

impl PlFactor for PVrCorr {
    #[inline]
    fn try_expr(&self) -> Result<Expr> {
        CLOSE.corr(VOLUME.pct(1), self.0).try_expr()
    }
}

#[ctor::ctor]
fn register() {
    register_pl_fac::<PVCorr>().unwrap();
    register_pl_fac::<PrVrCorr>().unwrap();
    register_pl_fac::<PrVCorr>().unwrap();
    register_pl_fac::<PVrCorr>().unwrap();
}
