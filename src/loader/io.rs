use std::fs::{self, File};
use std::io::{Read, Write};
use std::path::Path;
use std::sync::LazyLock;

use anyhow::{ensure, Result};
use bincode::{options, DefaultOptions, Options};

use super::DataLoader;
use crate::prelude::Frame;

pub(crate) static BINCODE_OPTIONS: LazyLock<DefaultOptions> = LazyLock::new(options);

impl DataLoader {
    pub fn save<P: AsRef<Path>>(&self, path: P) -> Result<()> {
        let path = path.as_ref();
        if path.extension().is_none() {
            return self.save_ipcs(path);
        }
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        let buf: Vec<u8> = BINCODE_OPTIONS.serialize(&self)?;
        let mut file = File::create(path)?;
        file.write_all(&buf)?;
        Ok(())
    }

    pub fn load<P: AsRef<Path>>(path: P) -> Result<Self> {
        let path = path.as_ref();
        if path.is_dir() {
            return DataLoader::read_ipcs(path, true);
        }
        let mut file = File::open(path)?;
        let mut buf = Vec::new();
        file.read_to_end(&mut buf)?;
        Ok(BINCODE_OPTIONS.deserialize(&buf)?)
    }

    pub fn save_ipcs<P: AsRef<Path>>(&self, path: P) -> Result<()> {
        use std::fs::File;

        use polars::io::SerWriter;
        use polars::prelude::IpcWriter;
        use rayon::prelude::*;
        let path = path.as_ref();
        ensure!(path.extension().is_none(), "path is not a directory");
        // remove old files
        if path.exists() {
            for entry in fs::read_dir(path)? {
                let entry = entry?;
                fs::remove_file(entry.path())?;
            }
        } else {
            fs::create_dir_all(path)?;
        }
        let base = self.empty_copy();
        base.save(path.join("__empty.dl"))?;
        self.par_iter().try_for_each(|(symbol, df)| -> Result<()> {
            let path = path.join(symbol.to_string() + ".ipc");
            let file = File::create(path)?;
            let mut df = df.clone().collect()?;
            IpcWriter::new(file)
                .with_compression(None)
                .finish(&mut df)?;
            Ok(())
        })?;
        Ok(())
    }

    pub fn read_ipcs<P: AsRef<Path>>(path: P, memory_map: bool) -> Result<Self> {
        use polars::prelude::{IpcReader, SerReader};
        use rayon::prelude::*;
        let path = path.as_ref();
        ensure!(path.is_dir(), "path is not a directory");
        let out = DataLoader::load(path.join("__empty.dl"))?;
        let dfs: Vec<Frame> = fs::read_dir(path)?
            .par_bridge()
            .filter_map(|file| {
                let file = file.unwrap();
                let file_path = file.path();
                if file_path.extension().map(|e| e == "ipc").unwrap_or(false) {
                    let file = File::open(&file_path).unwrap();
                    let mut reader = IpcReader::new(file);
                    if memory_map {
                        reader = reader.memory_mapped(Some(file_path))
                    }
                    return Some(reader.finish().unwrap().into());
                }
                None
            })
            .collect();
        Ok(out.with_dfs(dfs))
    }
}
