use std::sync::Arc;

use anyhow::{bail, Result};
use regex::Regex;

use super::{Param, PlFactor, PlFactorExt, TFactor, POLARS_FAC_MAP, T_FAC_MAP};

/// Parses a string representation of a Polars factor and returns the corresponding `PlFactor`.
///
/// # Arguments
///
/// * `name` - A string slice that holds the name of the factor, potentially including parameters.
///
/// # Returns
///
/// * `Result<Arc<dyn PlFactor>>` - An `Arc` containing the parsed `PlFactor` if successful, or an error if parsing fails.
pub fn parse_pl_fac(name: &str) -> Result<Arc<dyn PlFactor>> {
    let re = Regex::new(r"_\d+|\[.+\]|\(.*\)").unwrap();
    if re.is_match(name) {
        let name_parts = name.split("_").collect::<Vec<&str>>();
        let fac_name = name_parts[0..name_parts.len() - 1].join("_");
        let param = name_parts.last().unwrap();
        let param: Param = param.parse()?;
        let exists_flag = POLARS_FAC_MAP.lock().contains_key(fac_name.as_str());
        if exists_flag {
            let fac = POLARS_FAC_MAP.lock()[fac_name.as_str()](param);
            Ok(fac)
        } else {
            parse_pl_ext_fac(name)
        }
    } else {
        let exists_flag = POLARS_FAC_MAP.lock().contains_key(name);
        if exists_flag {
            let fac = POLARS_FAC_MAP.lock()[name](Param::None);
            Ok(fac)
        } else {
            parse_pl_ext_fac(name)
        }
    }
}

/// Parses a Polars extension factor from a string representation.
///
/// This function handles the parsing of extended Polars factors, which are composed of a base factor
/// and an extension method with a parameter.
///
/// # Arguments
///
/// * `name` - A string slice that holds the name of the extended factor, including the base factor,
///            extension method, and parameter.
///
/// # Returns
///
/// * `Result<Arc<dyn PlFactor>>` - An `Arc` containing the parsed extended `PlFactor` if successful,
///                                 or an error if parsing fails.
fn parse_pl_ext_fac(name: &str) -> Result<Arc<dyn PlFactor>> {
    if !name.contains('_') {
        bail!("Can not parse as polars extension factor: {}", name);
    }
    let mut name_parts = name.split("_").collect::<Vec<&str>>();
    let method_param = name_parts.pop().unwrap();
    let method_name = name_parts.pop().unwrap();
    let fac_name = name_parts.join("_");
    let method_param: Param = method_param.parse()?;
    let fac = parse_pl_fac(&fac_name)?;
    let fac: Arc<dyn PlFactor> = match method_name {
        "mean" => Arc::new(PlFactorExt::mean(fac, method_param)),
        "bias" => Arc::new(PlFactorExt::bias(fac, method_param)),
        "vol" => Arc::new(PlFactorExt::vol(fac, method_param)),
        "pure_vol" => Arc::new(PlFactorExt::pure_vol(fac, method_param)),
        "zscore" => Arc::new(PlFactorExt::zscore(fac, method_param)),
        "skew" => Arc::new(PlFactorExt::skew(fac, method_param)),
        "kurt" => Arc::new(PlFactorExt::kurt(fac, method_param)),
        "minmax" => Arc::new(PlFactorExt::minmax(fac, method_param)),
        "vol_rank" => Arc::new(PlFactorExt::vol_rank(fac, method_param)),
        "pct" => Arc::new(PlFactorExt::pct(fac, method_param)),
        "lag" => Arc::new(PlFactorExt::lag(fac, method_param)),
        "efficiency" => Arc::new(PlFactorExt::efficiency(fac, method_param)),
        "efficiency_sign" => Arc::new(PlFactorExt::efficiency_sign(fac, method_param)),
        _ => bail!(
            "Parse extension method: {} failed, not supported yet",
            method_name
        ),
    };
    Ok(fac)
}

/// Parses a string representation of a T factor and returns the corresponding `TFactor`.
///
/// # Arguments
///
/// * `name` - A string slice that holds the name of the factor, potentially including parameters.
///
/// # Returns
///
/// * `Result<Arc<dyn TFactor>>` - An `Arc` containing the parsed `TFactor` if successful, or an error if parsing fails.
pub fn parse_t_fac(name: &str) -> Result<Arc<dyn TFactor>> {
    let re = Regex::new(r"_\d+|\[.+\]|\(.*\)").unwrap();
    if re.is_match(name) {
        let name_parts = name.split("_").collect::<Vec<&str>>();
        let fac_name = name_parts[0..name_parts.len() - 1].join("_");
        let param = name_parts.last().unwrap();
        let param: Param = param.parse()?;
        let exists_flag = T_FAC_MAP.lock().contains_key(fac_name.as_str());
        if exists_flag {
            let fac = T_FAC_MAP.lock()[fac_name.as_str()](param);
            Ok(fac)
        } else {
            bail!("Factor {} not found", fac_name);
        }
    } else {
        let exists_flag = T_FAC_MAP.lock().contains_key(name);
        if exists_flag {
            let fac = T_FAC_MAP.lock()[name](Param::None);
            Ok(fac)
        } else {
            bail!("Factor {} not found", name);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_pl_fac() {
        let fac = parse_pl_fac("typ_1").unwrap();
        assert_eq!(fac.name(), "typ_1");
        let fac = parse_pl_fac("typ").unwrap();
        assert_eq!(fac.name(), "typ");
        assert!(parse_pl_fac("non_existent_factor").is_err());
    }

    #[test]
    fn test_parse_t_fac() {
        let fac = parse_t_fac("typ_1").unwrap();
        assert_eq!(fac.name(), "typ_1");
        let fac = parse_t_fac("typ").unwrap();
        assert_eq!(fac.name(), "typ");

        // Test with parameters
        let fac = parse_t_fac("typ").unwrap();
        assert_eq!(fac.name(), "typ");

        // Test with non-existent factor
        assert!(parse_t_fac("non_existent_factor").is_err());
    }

    #[test]
    #[cfg(feature = "map-fac")]
    fn test_parse_pl_fac_ext() {
        // Test mean extension
        let fac = parse_pl_fac("typ_mean_5").unwrap();
        assert_eq!(fac.name(), "typ_mean_5");

        // Test zscore extension
        let fac = parse_pl_fac("typ_zscore_10").unwrap();
        assert_eq!(fac.name(), "typ_zscore_10");

        // Test vol extension
        let fac = parse_pl_fac("typ_vol_20").unwrap();
        assert_eq!(fac.name(), "typ_vol_20");

        // Test lag extension
        let fac = parse_pl_fac("typ_lag_1").unwrap();
        assert_eq!(fac.name(), "typ_lag_1");

        // Test multiple extensions
        let fac = parse_pl_fac("typ_mean_5_zscore_10").unwrap();
        assert_eq!(fac.name(), "typ_mean_5_zscore_10");

        // Test with non-existent extension
        assert!(parse_pl_fac("typ_nonexistent_5").is_err());

        // Test with invalid parameter
        assert!(parse_pl_fac("typ_mean_invalid").is_err());
    }
}
