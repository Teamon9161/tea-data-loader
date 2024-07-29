use std::sync::Arc;

use anyhow::{bail, Result};
use regex::Regex;

use super::{Param, PlFactor, TFactor, POLARS_FAC_MAP, T_FAC_MAP};

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
            bail!("Factor {} not found", fac_name);
        }
    } else {
        let exists_flag = POLARS_FAC_MAP.lock().contains_key(name);
        if exists_flag {
            let fac = POLARS_FAC_MAP.lock()[name](Param::None);
            Ok(fac)
        } else {
            bail!("Factor {} not found", name);
        }
    }
}

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
    #[test]
    fn test_parse_pl_fac() {
        let fac = super::parse_pl_fac("typ_1").unwrap();
        assert_eq!(fac.name(), "typ_1");
        let fac = super::parse_pl_fac("typ").unwrap();
        assert_eq!(fac.name(), "typ");
    }
}
