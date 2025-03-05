use polars::prelude::*;
use tea_strategy::tevec::export::chrono::{Days, NaiveDate};

use super::load_kline::parse_rename_config;
use crate::path_finder::{PathConfig, PathFinder};
use crate::prelude::*;
use crate::utils::get_preprocess_exprs;

/// 判断交易是主买还是主卖
#[cfg(feature = "tick-fac")]
fn get_is_buy_expr() -> Expr {
    use crate::factors::tick::order_book::*;
    use crate::factors::tick::order_flow::*;
    use crate::factors::*;
    let is_buy = iif(ORDER_YTM.lt_eq(ASK1_YTM.shift(1)), true, NONE);
    let is_buy = iif(ORDER_YTM.gt_eq(BID1_YTM.shift(1)), false, is_buy);
    is_buy.expr().alias(IS_BUY.name())
}

#[cfg(feature = "tick-fac")]
fn get_amt_quantile(window: &'static str) -> Vec<Expr> {
    use crate::factors::tick::order_flow::*;
    const QUANTILES: [f64; 6] = [0.95, 0.9, 0.8, 0.5, 0.3, 0.2];
    QUANTILES
        .into_iter()
        .map(|q| {
            let f = OrderAmtQuantile(q, window);
            f.expr().alias(f.name())
        })
        .collect()
}

/// 根据price计算交易的ytm
fn get_trade_ytm(
    code_series: &Series,
    time_series: &Series,
    price_series: &Series,
) -> Result<Series> {
    use tea_bond::Bond;
    let code_series = code_series.str()?;
    let price_series = price_series.cast_f64()?;
    let date_series = time_series.cast(&DataType::Date)?;
    let mut bond: Option<Bond> = None;
    let ytm_series: Float64Chunked = itertools::izip!(
        date_series.date()?.into_iter(),
        code_series,
        price_series.f64()?
    )
    .map(|(date, code, price)| {
        if let Some(date) = date {
            let date = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap() + Days::new(date as u64);
            let code = code.unwrap();
            let current_code = if let Some(bond) = &bond {
                bond.code()
            } else {
                "__empty__"
            };
            if current_code != code {
                if let Ok(bond_read) = Bond::read_json(&format!("{}.IB", code), None) {
                    bond = Some(bond_read);
                } else {
                    bond = None;
                }
            }
            let res = bond.as_ref().map(|bond| {
                let cp_dates = bond.get_nearest_cp_date(date).unwrap();
                let accrued_interest = bond.calc_accrued_interest(date, Some(cp_dates)).unwrap();
                // 最后保留四位小数
                (bond
                    .calc_ytm_with_price(
                        price.unwrap_or(f64::NAN) + accrued_interest, // 全价
                        date,
                        Some(cp_dates),
                        None,
                    )
                    .unwrap_or(f64::NAN)
                    * 1_000_000.0) // 100 * 10000
                    .round()
                    / 10000.0
            });
            // 去除nan
            if let Some(res) = res {
                if !res.is_nan() {
                    return Some(res);
                }
            }
        }
        None
    })
    .collect();
    Ok(ytm_series.with_name("ytm".into()).into_series())
}

impl DataLoader {
    #[cfg(feature = "tick-fac")]
    /// 拼接trade数据
    pub fn with_trade_data(self) -> Result<Self> {
        let facs: Vec<Arc<dyn PlFactor>> = vec![];
        self.with_trade_data_and_facs(&facs)
    }

    #[cfg(feature = "tick-fac")]
    /// 使用trade数据计算因子（暂时去掉盘口数据，只保留真实成交数据），再拼接回原数据
    ///
    /// 使用本函数需要trade数据已经拼接完成，与with_trade_data_and_facs不同的是这种
    /// 情况下已经有is_buy列。
    pub fn with_trade_facs(mut self, facs: &[impl AsRef<dyn PlFactor>]) -> Result<Self> {
        use crate::factors::base::TIME;
        use crate::factors::tick::order_flow::*;
        if facs.is_empty() {
            return Ok(self);
        }
        let (trade_columns, time_col) = match &*self.typ {
            "ddb-xbond" => (
                vec![
                    ORDER_TIME.name(),
                    ORDER_VOL.name(),
                    ORDER_PRICE.name(),
                    ORDER_YTM.name(),
                    ORDER_AMT.name(),
                    IS_BUY.name(),
                ],
                ORDER_TIME.name(),
            ),
            "sse-bond" => (
                vec![
                    TIME.name(),
                    ORDER_VOL.name(),
                    ORDER_PRICE.name(),
                    ORDER_AMT.name(),
                    IS_BUY.name(),
                ],
                TIME.name(),
            ),
            tp => todo!("with trade facs is not supported for type: {}", tp),
        };
        let fac_names = facs.iter().map(|f| f.as_ref().name()).collect::<Vec<_>>();
        let mut trade_dl = self
            .clone()
            .filter(col(ORDER_VOL.name()).is_not_null())?
            .select([cols(trade_columns)])?;
        trade_dl = trade_dl
            .with_pl_facs(facs)?
            .select([col(&time_col), cols(&fac_names)])?;
        self.dfs = self
            .dfs
            .into_iter()
            .zip(trade_dl.dfs)
            .map(|(df, trade_df)| df.left_join(trade_df, col(&time_col), col(&time_col)))
            .collect::<Result<Frames>>()?;
        Ok(self)
    }

    #[cfg(feature = "tick-fac")]
    /// 在首次拼接到盘口数据前就先计算好订单流因子，但是由于尚未拼接盘口数据，
    /// 无法得到交易是主买还是主卖（无is_buy列）
    fn with_trade_data_and_facs(self, facs: &[impl AsRef<dyn PlFactor>]) -> Result<Self> {
        use crate::factors::tick::order_flow::*;
        ensure!(
            &*self.typ == "ddb-xbond",
            "trade data only support ddb-xbond"
        );
        let path_config = PathConfig::new(&self.typ, "trade");
        let path = PathFinder::new(path_config)?.path()?;
        let rename_map =
            parse_rename_config(&CONFIG.loader.rename, Some(&self.typ), Some("trade"), None);
        let filter_cond = self.time_filter_cond("tick")?;
        let preprocess_exprs = get_preprocess_exprs(&self.typ, "trade");
        let mut trade_df = LazyFrame::scan_ipc(path, Default::default())?;
        if let Some(map) = rename_map {
            trade_df = trade_df.rename(map.keys(), map.values().map(|v| v.as_str().unwrap()), false)
        }
        // apply filter condition
        if let Some(cond) = filter_cond.clone() {
            trade_df = trade_df.filter(cond)
        };
        let order_vol = ORDER_VOL.expr();
        let order_price = ORDER_PRICE.expr();
        let order_ytm = ORDER_YTM.expr();
        let mut trade_df = trade_df
            .with_columns(&preprocess_exprs)
            .group_by_stable([col("symbol"), col("time")])
            .agg([
                order_vol.clone().sum().alias(ORDER_VOL.name()),
                ((order_price.clone() * order_vol.clone()).sum() / order_vol.sum())
                    .alias(ORDER_PRICE.name()),
                when(order_ytm.clone().count().eq(1))
                    .then(order_ytm.clone().first())
                    .otherwise(NULL.lit())
                    .alias(ORDER_YTM.name()),
            ])
            .collect()?;
        // 对于均价，推断出其对应的ytm
        trade_df.with_column(
            get_trade_ytm(
                trade_df.column("symbol").unwrap().as_materialized_series(),
                trade_df.column("time").unwrap().as_materialized_series(),
                trade_df
                    .column(&ORDER_PRICE.name())
                    .unwrap()
                    .as_materialized_series(),
            )?
            .with_name("infer_ytm".into()),
        )?;

        // trade df预处理
        let trade_df = trade_df
            .into_frame()
            .with_column(
                (ORDER_VOL.expr() * ORDER_PRICE.expr() * 100_000.lit())
                    .cast(DataType::Float64)
                    .alias("order_amt"),
            )?
            .with_columns(get_amt_quantile("5d"))?
            .with_columns([
                when(order_ytm.clone().is_null())
                    .then(col("infer_ytm"))
                    .otherwise(order_ytm)
                    .alias(ORDER_YTM.name()),
                col("time").alias("order_time"),
                ORDER_VOL.expr().cast(DataType::Float64), // 调整数据类型，i32类型如果对较长窗口滚动求和，可能会溢出
            ])?
            .drop(["infer_ytm"])?;
        // 拼接trade_df
        let mut out = self.empty_copy();
        let fac_names = facs.iter().map(|f| f.as_ref().name()).collect::<Vec<_>>();
        let trade_columns = [
            ORDER_TIME.name(),
            ORDER_VOL.name(),
            ORDER_PRICE.name(),
            ORDER_YTM.name(),
            ORDER_AMT.name(),
        ];
        out.dfs = self
            .into_iter()
            .map(|(symbol, df)| {
                let current_trade_df = trade_df
                    .clone()
                    .filter(col("symbol").eq(symbol.lit()))?
                    .select([col("*").exclude(["symbol"])])?
                    .with_pl_facs(facs)?;
                let df = df.select([col("*").exclude(&trade_columns)])?.join(
                    current_trade_df,
                    [col("time")],
                    [col("time")],
                    JoinArgs {
                        how: JoinType::AsOf(AsOfOptions {
                            strategy: AsofStrategy::Backward,
                            // 通常交易的时间戳要先于order_book的时间戳，基本差距在1s以内
                            tolerance_str: Some("2s".into()),
                            ..Default::default()
                        }),
                        ..Default::default()
                    },
                )?;
                // 对于同一笔交易，保证只拼到第一个盘口
                let ot = col(ORDER_TIME.name());
                let duplicate_cond = ot
                    .clone()
                    .eq(ot.clone().shift(1.lit()))
                    .and(ot.is_not_null());
                df.with_columns(
                    trade_columns
                        .iter()
                        .chain(&fac_names)
                        .map(|f| {
                            when(duplicate_cond.clone())
                                .then(NULL.lit())
                                .otherwise(col(f))
                                .alias(f)
                        })
                        .collect::<Vec<_>>(),
                )?
                .with_column(get_is_buy_expr())
            })
            .collect::<Result<Frames>>()?;
        Ok(out)
    }
}
