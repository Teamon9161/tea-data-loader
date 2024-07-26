use std::sync::Arc;

use anyhow::{bail, Result};

use super::{Param, PlFactor, POLARS_FAC_MAP};

pub fn parse_pl_fac(name: &str) -> Result<Arc<dyn PlFactor>> {
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
}
