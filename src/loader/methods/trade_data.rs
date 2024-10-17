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
    is_buy.expr().alias("is_buy")
}

#[cfg(feature = "tick-fac")]
fn get_vol_quantile(window: &'static str) -> Vec<Expr> {
    use crate::factors::tick::order_flow::*;
    const QUANTILES: [f64; 6] = [0.95, 0.9, 0.8, 0.5, 0.3, 0.2];
    QUANTILES
        .into_iter()
        .map(|q| {
            let f = OrderAmtQuantile(q, window);
            f.expr().alias(&f.name())
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
            let current_code = if bond.is_some() {
                bond.as_ref().unwrap().code()
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
                    * 1000_000.0) // 100 * 10000
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
    Ok(ytm_series.with_name("ytm").into_series())
}

impl DataLoader {
    #[cfg(feature = "tick-fac")]
    pub fn with_trade_data(self, memory_map: bool) -> Result<Self> {
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
        let mut trade_df = LazyFrame::scan_ipc(
            path,
            ScanArgsIpc {
                memory_map,
                ..Default::default()
            },
        )?;
        if let Some(map) = rename_map {
            trade_df = trade_df.rename(map.keys(), map.values().map(|v| v.as_str().unwrap()))
        }
        // apply filter condition
        if let Some(cond) = filter_cond.clone() {
            trade_df = trade_df.filter(cond)
        };
        let order_amt = ORDER_AMT.expr();
        let order_price = ORDER_PRICE.expr();
        let order_ytm = ORDER_YTM.expr();
        let mut trade_df = trade_df
            .with_columns(&preprocess_exprs)
            .group_by_stable([col("symbol"), col("time")])
            .agg([
                order_amt.clone().sum().alias(&ORDER_AMT.name()),
                ((order_price.clone() * order_amt.clone()).sum() / order_amt.sum())
                    .alias(&ORDER_PRICE.name()),
                when(order_ytm.clone().count().eq(1))
                    .then(order_ytm.clone().first())
                    .otherwise(NULL.lit())
                    .alias(&ORDER_YTM.name()),
            ])
            .collect()?;
        // 对于均价，推断出其对应的ytm
        trade_df.with_column(
            get_trade_ytm(
                trade_df.column("symbol").unwrap(),
                trade_df.column("time").unwrap(),
                trade_df.column(&ORDER_PRICE.name()).unwrap(),
            )?
            .with_name("infer_ytm"),
        )?;

        let trade_df = trade_df
            .into_frame()
            .with_columns(get_vol_quantile("5d"))?
            .with_columns([
                when(order_ytm.clone().is_null())
                    .then(col("infer_ytm"))
                    .otherwise(order_ytm)
                    .alias(&ORDER_YTM.name()),
                col("time").alias("order_time"),
                ORDER_AMT.expr().cast(DataType::Float64), // 调整数据类型，i32类型如果对较长窗口滚动求和，可能会溢出
            ])?
            .drop(["infer_ytm"])?;
        // 拼接trade_df
        let mut out = self.empty_copy();
        out.dfs = self
            .into_iter()
            .map(|(symbol, df)| {
                let df = df.join(
                    trade_df
                        .clone()
                        .filter(col("symbol").eq(symbol.lit()))?
                        .select([col("*").exclude(["symbol"])])?,
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
                let trade_columns = [
                    ORDER_TIME.name(),
                    ORDER_AMT.name(),
                    ORDER_PRICE.name(),
                    ORDER_YTM.name(),
                ];
                let ot = col(&ORDER_TIME.name());
                let duplicate_cond = ot
                    .clone()
                    .eq(ot.clone().shift(1.lit()))
                    .and(ot.is_not_null());
                df.with_columns(
                    trade_columns
                        .into_iter()
                        .map(|f| {
                            when(duplicate_cond.clone())
                                .then(NULL.lit())
                                .otherwise(col(&f))
                                .alias(&f)
                        })
                        .collect::<Vec<_>>(),
                )?
                // .with_column(col("order_amt_quantile_*").forward_fill(None))?
                .with_column(get_is_buy_expr())
            })
            .collect::<Result<Frames>>()?;
        Ok(out)
    }
}
