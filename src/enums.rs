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
