/// 复权方式
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Adjust {
    None,
    Pre,
    Post,
}

/// 合约活跃程度
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Tier {
    Lead,
    SubLead,
    All,
    None,
}

impl Adjust {
    #[inline]
    pub fn as_str(&self) -> &str {
        match self {
            Adjust::None => "none",
            Adjust::Pre => "pre",
            Adjust::Post => "post",
        }
    }
}

impl Tier {
    #[inline]
    pub fn as_str(&self) -> &str {
        match self {
            Tier::Lead => "hot",
            Tier::SubLead => "subhot",
            Tier::All => "all",
            Tier::None => "none",
        }
    }
}

/// 聚合方法
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AggMethod {
    Mean,
    WeightMean(std::sync::Arc<str>), // 通过权重字段加权平均
    Max,
    Min,
    Sum,
    ValidFirst, // currently not yet implemented as polarsr doesn't support a valid first horizontal expression
    First,
    Last,
}

/// 手续费
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum CRate {
    Percent(f64),  // 使用百分比计算的手续费
    Absolute(f64), // 固定按一手多少手续费计算
}

impl Default for CRate {
    #[inline]
    fn default() -> Self {
        Self::Percent(0.0003)
    }
}

impl CRate {
    #[inline]
    pub fn get_type(&self) -> tea_strategy::equity::CommissionType {
        match self {
            CRate::Percent(_) => tea_strategy::equity::CommissionType::Percent,
            CRate::Absolute(_) => tea_strategy::equity::CommissionType::Absolute,
        }
    }

    #[inline]
    pub fn get_value(&self) -> f64 {
        match self {
            CRate::Percent(v) => *v,
            CRate::Absolute(v) => *v,
        }
    }
}
