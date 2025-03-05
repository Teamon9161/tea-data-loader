mod abs;
mod bias;
mod compare;
mod corr;
mod cum_sum;
mod diff;
mod efficiency;
mod efficiency_sign;
mod ewm;
mod fill;
mod iif;
mod imbalance;
mod is_none;
mod kurt;
mod log;
mod max;
mod mean;
mod min;
mod minmax;
mod pct;
mod pure_vol;
mod shift;
mod skew;
mod sum;
mod vol;
mod vol_rank;
mod zscore;

use crate::base::Null;
use crate::prelude::*;

pub type BiasFactor<F> = Factor<bias::FactorBias<F>>;
pub type AbsFactor<F> = Factor<abs::FactorAbs<F>>;
pub type IsNoneFactor<F> = Factor<is_none::FactorIsNone<F>>;
pub type NotNoneFactor<F> = Factor<is_none::FactorNotNone<F>>;
pub type EfficiencySignFactor<F> = Factor<efficiency_sign::FactorEfficiencySign<F>>;
pub type EfficiencyFactor<F> = Factor<efficiency::FactorEfficiency<F>>;
pub type ImbalanceFactor<F, G> = Factor<imbalance::FactorImbalance<F, G>>;
pub type MeanFactor<F> = Factor<mean::FactorMean<F>>;
pub type MinmaxFactor<F> = Factor<minmax::FactorMinmax<F>>;
pub type PctFactor<F> = Factor<pct::FactorPct<F>>;
pub type PureVolFactor<F> = Factor<pure_vol::FactorPureVol<F>>;
pub type ShiftFactor<F> = Factor<shift::FactorShift<F>>;
pub type SkewFactor<F> = Factor<skew::FactorSkew<F>>;
pub type VolFactor<F> = Factor<vol::FactorVol<F>>;
pub type SumFactor<F> = Factor<sum::FactorSum<F>>;
pub type VolRankFactor<F> = Factor<vol_rank::FactorVolRank<F>>;
pub type ZscoreFactor<F> = Factor<zscore::FactorZscore<F>>;
pub type KurtFactor<F> = Factor<kurt::FactorKurt<F>>;
pub type DiffFactor<F> = Factor<diff::FactorDiff<F>>;
pub type MinFactor<F> = Factor<min::FactorMin<F>>;
pub type MaxFactor<F> = Factor<max::FactorMax<F>>;
pub type EwmFactor<F> = Factor<ewm::FactorEwm<F>>;
pub type LogFactor<F> = Factor<log::FactorLog<F>>;
pub type CorrFactor<F, G> = Factor<corr::FactorCorr<F, G>>;
pub type CumSumFactor<F> = Factor<cum_sum::FactorCumSum<F>>;
pub use compare::FactorCmpExt;
pub use iif::iif;
use polars::prelude::FillNullStrategy;

/// Extension trait for factors providing additional methods for factor manipulation and analysis.
///
/// This trait is implemented for all types that implement `FactorBase`, providing a rich set of
/// methods to transform and analyze factors in various ways.
pub trait FactorExt: FactorBase {
    #[inline]
    /// Calculates the absolute value of the factor.
    fn abs(self) -> AbsFactor<Self> {
        abs::FactorAbs(self).into()
    }

    #[inline]
    #[allow(clippy::wrong_self_convention)]
    fn is_none(self) -> IsNoneFactor<Self> {
        is_none::FactorIsNone(self).into()
    }

    #[inline]
    #[allow(clippy::wrong_self_convention)]
    fn is_not_none(self) -> NotNoneFactor<Self> {
        is_none::FactorNotNone(self).into()
    }

    #[inline]
    /// Calculates the logarithm of the factor.
    fn log(self, base: f64) -> LogFactor<Self> {
        log::FactorLog { fac: self, base }.into()
    }

    #[inline]
    /// Calculates the logarithm of the factor with base e.
    fn ln(self) -> LogFactor<Self> {
        use std::f64::consts::E;
        log::FactorLog { fac: self, base: E }.into()
    }

    /// Calculates the bias of the factor relative to its rolling mean.
    ///
    /// The bias is computed as: (factor / rolling_mean(factor)) - 1
    ///
    /// # Arguments
    ///
    /// * `param` - The parameter for the rolling window size as a fixed number of periods.
    ///
    /// # Returns
    ///
    /// Returns a `BiasFactor<Self>` instance representing the bias calculation.
    #[inline]
    fn bias(self, param: usize) -> BiasFactor<Self> {
        bias::FactorBias {
            fac: self,
            param,
            min_periods: None,
        }
        .into()
    }

    /// Fills null values in the factor using forward fill strategy.
    ///
    /// This method replaces null values with the last non-null value that came before them.
    ///
    /// # Returns
    ///
    /// Returns a `Factor<fill::FactorFill<Self, Null>>` instance with forward fill strategy applied.
    #[inline]
    fn ffill(self) -> Factor<fill::FactorFill<Self, Null>> {
        fill::FactorFill {
            fac: self,
            strategy: Some(FillNullStrategy::Forward(None)),
            value: None,
        }
        .into()
    }

    /// Fills null values in the factor using backward fill strategy.
    ///
    /// This method replaces null values with the next non-null value that comes after them.
    ///
    /// # Returns
    ///
    /// Returns a `Factor<fill::FactorFill<Self, Null>>` instance with backward fill strategy applied.
    #[inline]
    fn bfill(self) -> Factor<fill::FactorFill<Self, Null>> {
        fill::FactorFill {
            fac: self,
            strategy: Some(FillNullStrategy::Backward(None)),
            value: None,
        }
        .into()
    }

    /// Fills null values in the factor with a specified value.
    ///
    /// # Arguments
    ///
    /// * `value` - The value to use for filling null values. It must implement `FactorBase`.
    ///
    /// # Returns
    ///
    /// Returns a `Factor<fill::FactorFill<Self, G>>` instance with the specified value used for filling null values.
    #[inline]
    fn fill<G: FactorBase>(self, value: G) -> Factor<fill::FactorFill<Self, G>> {
        fill::FactorFill {
            fac: self,
            strategy: None,
            value: Some(value),
        }
        .into()
    }

    /// Fills null values in the factor using a specified strategy.
    ///
    /// This method allows for more flexible null-filling strategies compared to `ffill`, `bfill`, or `fill`.
    ///
    /// # Arguments
    ///
    /// * `strategy` - A `FillNullStrategy` enum value specifying the strategy to use for filling null values.
    ///
    /// # Returns
    ///
    /// Returns a `Factor<fill::FactorFill<Self, Null>>` instance with the specified strategy applied for filling null values.
    #[inline]
    fn fill_null_with_strategy(
        self,
        strategy: FillNullStrategy,
    ) -> Factor<fill::FactorFill<Self, Null>> {
        fill::FactorFill {
            fac: self,
            strategy: Some(strategy),
            value: None,
        }
        .into()
    }

    /// Calculates the correlation between the current factor and another factor.
    ///
    /// # Arguments
    ///
    /// * `other` - Another factor to compare with this factor.
    /// * `window` - The window size for the correlation calculation.
    ///
    /// # Returns
    ///
    /// Returns a `CorrFactor<Self, G>` instance representing the correlation between the two factors.
    #[inline]
    fn corr<G: FactorBase>(self, other: G, window: usize) -> CorrFactor<Self, G> {
        corr::FactorCorr {
            left: self,
            right: other,
            window,
            min_periods: None,
        }
        .into()
    }

    /// Calculates the cumulative sum of the factor.
    ///
    /// This method computes the cumulative sum of the factor values over a specified window.
    ///
    /// # Returns
    ///
    /// Returns a `CumSumFactor<Self>` instance representing the cumulative sum of the factor.
    #[inline]
    fn cum_sum(self) -> CumSumFactor<Self> {
        cum_sum::FactorCumSum(self).into()
    }

    /// Calculates the difference between the current value and a lagged value of the factor.
    ///
    /// This method computes the difference between the current value of the factor and its value
    /// at a specified lag, which is useful for measuring changes or trends over time.
    ///
    /// # Arguments
    ///
    /// * `param` - An integer representing the lag period for the difference calculation.
    ///
    /// # Returns
    ///
    /// Returns a `DiffFactor<Self>` instance representing the difference calculation.
    #[inline]
    fn diff(self, param: i64) -> DiffFactor<Self> {
        diff::FactorDiff { fac: self, param }.into()
    }

    /// Calculates the signed efficiency ratio of the factor.
    ///
    /// The efficiency sign is a measure of the factor's directional consistency over time.
    ///
    /// # Arguments
    ///
    /// * `param` - A value that can be converted into a `Param`, representing the window size for the efficiency sign calculation.
    ///
    /// # Returns
    ///
    /// Returns an `EfficiencySignFactor<Self>` instance representing the signed efficiency ratio.
    #[inline]
    fn efficiency_sign(self, param: usize) -> EfficiencySignFactor<Self> {
        efficiency_sign::FactorEfficiencySign {
            fac: self,
            param,
            min_periods: None,
        }
        .into()
    }

    /// Calculates the efficiency of the factor.
    ///
    /// The efficiency factor measures how effectively the factor moves in a consistent direction over time.
    ///
    /// # Arguments
    ///
    /// * `param` - A value that can be converted into a `Param`, representing the window size for the efficiency calculation.
    ///
    /// # Returns
    ///
    /// Returns an `EfficiencyFactor<Self>` instance representing the factor's efficiency.
    #[inline]
    fn efficiency(self, param: usize) -> EfficiencyFactor<Self> {
        efficiency::FactorEfficiency {
            param,
            fac: self,
            min_periods: None,
        }
        .into()
    }

    /// Calculates the Exponentially Weighted Moving Average (EWM) of the factor.
    ///
    /// # Arguments
    ///
    /// * `param` - The parameter for the smoothing factor, typically a value between 0 and 1.
    ///
    /// # Returns
    ///
    /// Returns an `EwmFactor<Self>` instance representing the EWM of the factor.
    #[inline]
    fn ewm(self, param: usize) -> EwmFactor<Self> {
        ewm::FactorEwm {
            fac: self,
            param,
            min_periods: None,
        }
        .into()
    }

    /// Calculates the imbalance between this factor and another factor.
    ///
    /// The imbalance factor measures the relative difference or disparity between two factors.
    ///
    /// # Arguments
    ///
    /// * `other` - Another factor to compare with this factor.
    ///
    /// # Returns
    ///
    /// Returns an `ImbalanceFactor<Self, G>` instance representing the imbalance between the two factors.
    #[inline]
    fn imb<G: FactorBase>(self, other: G) -> ImbalanceFactor<Self, G> {
        imbalance::FactorImbalance {
            left: self,
            right: other,
        }
        .into()
    }

    /// An alias for the `imb` method, providing a more descriptive name.
    ///
    /// This method has the same functionality as `imb`, calculating the imbalance between two factors.
    ///
    /// # Arguments
    ///
    /// * `other` - Another factor to compare with this factor.
    ///
    /// # Returns
    ///
    /// Returns an `ImbalanceFactor<Self, G>` instance representing the imbalance between the two factors.
    #[inline]
    fn imbalance<G: FactorBase>(self, other: G) -> ImbalanceFactor<Self, G> {
        self.imb(other)
    }

    /// Calculates the rolling mean of the factor.
    ///
    /// # Arguments
    ///
    /// * `param` - A parameter that can be converted into `Param`, specifying the
    ///   period over which to calculate the mean.
    ///
    /// # Returns
    ///
    /// Returns a `MeanFactor<Self>` instance representing the rolling mean of the factor.
    #[inline]
    fn mean(self, param: usize) -> MeanFactor<Self> {
        mean::FactorMean {
            param,
            fac: self,
            min_periods: None,
        }
        .into()
    }

    /// Calculates the rolling mean of the factor with optional minimum periods.
    ///
    /// # Arguments
    ///
    /// * `param` - A parameter that can be converted into `Param`, specifying the
    ///   period over which to calculate the mean.
    /// * `min_periods` - The minimum number of periods required to have a value.
    ///
    /// # Returns
    ///
    /// Returns a `MeanFactor<Self>` instance representing the rolling mean of the factor.
    #[inline]
    fn mean_opt(self, param: usize, min_periods: usize) -> MeanFactor<Self> {
        mean::FactorMean {
            param,
            fac: self,
            min_periods: Some(min_periods),
        }
        .into()
    }

    /// Calculates the rolling sum of the factor.
    ///
    /// # Arguments
    ///
    /// * `param` - A parameter that can be converted into `Param`, specifying the
    ///   window size for the rolling sum.
    ///
    /// # Returns
    ///
    /// Returns a `SumFactor<Self>` instance representing the rolling sum of the factor.
    #[inline]
    fn sum(self, param: usize) -> SumFactor<Self> {
        sum::FactorSum {
            param,
            fac: self,
            min_periods: None,
        }
        .into()
    }

    /// Calculates the rolling sum of the factor with optional minimum periods.
    ///
    /// # Arguments
    ///
    /// * `param` - A parameter that can be converted into `Param`, specifying the
    ///   window size for the rolling sum.
    /// * `min_periods` - The minimum number of periods required to have a value.
    ///
    /// # Returns
    ///
    /// Returns a `SumFactor<Self>` instance representing the rolling sum of the factor.
    #[inline]
    fn sum_opt(self, param: usize, min_periods: usize) -> SumFactor<Self> {
        sum::FactorSum {
            param,
            fac: self,
            min_periods: Some(min_periods),
        }
        .into()
    }

    /// Calculates the rolling maximum of the factor.
    ///
    /// # Arguments
    ///
    /// * `param` - A parameter that can be converted into `Param`, specifying the
    ///   window size for the rolling maximum.
    ///
    /// # Returns
    ///
    /// Returns a `MaxFactor<Self>` instance representing the rolling maximum of the factor.
    #[inline]
    fn max(self, param: usize) -> MaxFactor<Self> {
        max::FactorMax {
            param,
            fac: self,
            min_periods: None,
        }
        .into()
    }

    /// Calculates the rolling minimum of the factor.
    ///
    /// # Arguments
    ///
    /// * `param` - A parameter that can be converted into `Param`, specifying the
    ///   window size for the rolling minimum.
    ///
    /// # Returns
    ///
    /// Returns a `MinFactor<Self>` instance representing the rolling minimum of the factor.
    #[inline]
    fn min(self, param: usize) -> MinFactor<Self> {
        min::FactorMin {
            param,
            fac: self,
            min_periods: None,
        }
        .into()
    }

    /// Applies rolling min-max normalization to the factor.
    ///
    /// This method normalizes the factor values to a range between 0 and 1 based on
    /// the minimum and maximum values within a rolling window.
    ///
    /// # Arguments
    ///
    /// * `param` - A parameter that can be converted into `Param`, specifying the
    ///   window size for the rolling normalization.
    ///
    /// # Returns
    ///
    /// Returns a `MinmaxFactor<Self>` instance representing the min-max normalized factor.
    #[inline]
    fn minmax(self, param: usize) -> MinmaxFactor<Self> {
        minmax::FactorMinmax {
            param,
            min_periods: None,
            fac: self,
        }
        .into()
    }

    /// Calculates the percentage change of the factor.
    ///
    /// # Arguments
    ///
    /// * `param` - A parameter that can be converted into `Param`, representing the period
    ///   over which the percentage change is calculated.
    ///
    /// # Returns
    ///
    /// Returns a `PctFactor<Self>` instance representing the percentage change of the factor.
    #[inline]
    fn pct(self, param: i64) -> PctFactor<Self> {
        pct::FactorPct { param, fac: self }.into()
    }

    /// Calculates the pure volatility of the factor.
    ///
    /// Pure volatility is defined as the standard deviation divided by the mean.
    ///
    /// # Arguments
    ///
    /// * `param` - A parameter that can be converted into `Param`, used for volatility calculation.
    ///
    /// # Returns
    ///
    /// Returns a `PureVolFactor<Self>` representing the pure volatility of the factor.
    #[inline]
    fn pure_vol(self, param: usize) -> PureVolFactor<Self> {
        pure_vol::FactorPureVol {
            param,
            fac: self,
            min_periods: None,
        }
        .into()
    }

    /// Creates a shifted version of the factor.
    ///
    /// This method shifts the factor values by a specified number of periods.
    ///
    /// # Arguments
    ///
    /// * `param` - The parameter to shift the factor by. This can be any type
    ///   that can be converted into a `Param`.
    ///
    /// # Returns
    ///
    /// Returns a `ShiftFactor<Self>` instance representing the shifted factor.
    #[inline]
    fn shift(self, param: i64) -> ShiftFactor<Self> {
        shift::FactorShift { param, fac: self }.into()
    }

    /// Calculates the rolling skewness of the factor.
    ///
    /// Skewness measures the asymmetry of the probability distribution of the factor values.
    ///
    /// # Arguments
    ///
    /// * `param` - A parameter that can be converted into `Param`, representing the window size for the skewness calculation.
    ///
    /// # Returns
    ///
    /// Returns a `SkewFactor<Self>` instance representing the rolling skewness of the factor.
    #[inline]
    fn skew(self, param: usize) -> SkewFactor<Self> {
        skew::FactorSkew {
            param,
            fac: self,
            min_periods: None,
        }
        .into()
    }

    /// Calculates the rolling kurtosis of the factor.
    ///
    /// Kurtosis measures the "tailedness" of the probability distribution of the factor values.
    ///
    /// # Arguments
    ///
    /// * `param` - A parameter that can be converted into `Param`, representing the window size for the kurtosis calculation.
    ///
    #[inline]
    fn kurt(self, param: usize) -> KurtFactor<Self> {
        kurt::FactorKurt {
            param,
            fac: self,
            min_periods: None,
        }
        .into()
    }

    /// Calculates the rolling volatility rank of the factor.
    ///
    /// This method computes the rank of the factor's volatility over a specified window.
    ///
    /// # Arguments
    ///
    /// * `param` - A parameter that can be converted into `Param`, representing the window size for the volatility rank calculation.
    ///
    /// # Returns
    ///
    /// Returns a `VolRankFactor<Self>` instance representing the rolling volatility rank of the factor.
    #[inline]
    fn vol_rank(self, param: usize) -> VolRankFactor<Self> {
        vol_rank::FactorVolRank {
            param,
            fac: self,
            min_periods: None,
        }
        .into()
    }

    /// Calculates the rolling standard deviation (volatility) of the factor.
    ///
    /// # Arguments
    ///
    /// * `param` - A parameter that can be converted into `Param`, typically representing the window size for volatility calculation.
    ///
    /// # Returns
    ///
    /// Returns a `VolFactor<Self>` instance representing the volatility of the factor.
    #[inline]
    fn vol(self, param: usize) -> VolFactor<Self> {
        vol::FactorVol {
            param,
            fac: self,
            min_periods: None,
        }
        .into()
    }

    /// An alias for the `vol` method, calculating the standard deviation of the factor.
    ///
    /// This method has the same functionality as `vol`, providing a more statistically familiar name.
    ///
    /// # Arguments
    ///
    /// * `param` - A parameter that can be converted into `Param`, typically representing the window size for standard deviation calculation.
    ///
    /// # Returns
    ///
    /// Returns a `VolFactor<Self>` instance representing the standard deviation of the factor.
    #[inline]
    fn std(self, param: usize) -> VolFactor<Self> {
        self.vol(param)
    }

    /// Calculates the rolling z-score of the factor.
    ///
    /// The z-score is computed by subtracting the mean and dividing by the standard deviation
    /// over a specified window. It measures how many standard deviations a data point is from the mean.
    ///
    /// # Arguments
    ///
    /// * `param` - The parameter specifying the window size for the z-score calculation.
    ///
    /// # Returns
    ///
    /// Returns a `ZscoreFactor<Self>` representing the z-score of the factor.
    #[inline]
    fn zscore(self, param: usize) -> ZscoreFactor<Self> {
        zscore::FactorZscore {
            param,
            fac: self,
            min_periods: None,
        }
        .into()
    }
}

impl<F: FactorBase> FactorExt for F {}
