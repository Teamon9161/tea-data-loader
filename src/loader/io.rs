use std::fs::{self, File};
use std::io::{Read, Write};
use std::path::Path;
use std::sync::{Arc, LazyLock};

use anyhow::{ensure, Result};
use bincode::{options, DefaultOptions, Options};

use super::DataLoader;
use crate::prelude::Frame;

pub(crate) static BINCODE_OPTIONS: LazyLock<DefaultOptions> = LazyLock::new(options);

/// Implementation of I/O operations for the `DataLoader` struct.
impl DataLoader {
    /// Saves the `DataLoader` data to a file or directory.
    ///
    /// # Arguments
    ///
    /// * `path` - The path where the data should be saved.
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` if the save operation is successful, otherwise returns an error.
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

    /// Loads `DataLoader` data from a file or directory.
    ///
    /// # Arguments
    ///
    /// * `path` - The path from where the data should be loaded.
    /// * `lazy` - Whether to load the data lazily.
    ///
    /// # Returns
    ///
    /// Returns a `Result` containing the loaded `DataLoader` if successful, otherwise returns an error.
    #[inline]
    pub fn load<P: AsRef<Path>>(path: P, lazy: bool) -> Result<Self> {
        let path = path.as_ref();
        if path.is_dir() {
            return DataLoader::read_ipcs(path, None, true, lazy);
        }
        let mut file = File::open(path)?;
        let mut buf = Vec::new();
        file.read_to_end(&mut buf)?;
        Ok(BINCODE_OPTIONS.deserialize(&buf)?)
    }

    /// Loads specific symbols from a `DataLoader` file or directory.
    ///
    /// # Arguments
    ///
    /// * `path` - The path from where the data should be loaded.
    /// * `symbols` - A slice of symbols to load.
    /// * `lazy` - Whether to load the data lazily.
    ///
    /// # Returns
    ///
    /// Returns a `Result` containing the loaded `DataLoader` if successful, otherwise returns an error.
    #[inline]
    pub fn load_symbols<P: AsRef<Path>, S: AsRef<str>>(
        path: P,
        symbols: &[S],
        lazy: bool,
    ) -> Result<Self> {
        if path.as_ref().is_dir() {
            let symbols = symbols.iter().map(|s| s.as_ref()).collect::<Vec<_>>();
            return DataLoader::read_ipcs(path, Some(&symbols), true, lazy);
        }
        DataLoader::load(path, lazy)
    }

    /// Saves the `DataLoader` data to a directory in IPC (Arrow IPC) format.
    ///
    /// # Arguments
    ///
    /// * `path` - The directory path where the data should be saved.
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` if the save operation is successful, otherwise returns an error.
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
            let path = path.join(symbol.to_string() + ".feather");
            let file = File::create(path)?;
            let mut df = df.clone().collect()?;
            IpcWriter::new(file)
                .with_compression(None)
                .finish(&mut df)?;
            Ok(())
        })?;
        Ok(())
    }

    /// Reads `DataLoader` data from a directory in IPC (Arrow IPC) format.
    ///
    /// # Arguments
    ///
    /// * `path` - The directory path from where the data should be read.
    /// * `symbols` - Optional slice of symbols to read.
    /// * `memory_map` - Whether to use memory mapping when reading the files.
    /// * `lazy` - Whether to load the data lazily.
    ///
    /// # Returns
    ///
    /// Returns a `Result` containing the loaded `DataLoader` if successful, otherwise returns an error.
    pub fn read_ipcs<P: AsRef<Path>>(
        path: P,
        symbols: Option<&[&str]>,
        memory_map: bool,
        lazy: bool,
    ) -> Result<Self> {
        use polars::prelude::*;
        use rayon::prelude::*;
        let path = path.as_ref();
        ensure!(path.is_dir(), "path is not a directory");
        let config_path = path.join("__empty.dl");
        let mut out = if config_path.exists() {
            DataLoader::load(config_path, false)?
        } else {
            DataLoader::new("")
        };
        let (find_symbols, dfs): (Vec<Arc<str>>, Vec<Frame>) = if let Some(symbols) = symbols {
            symbols
                .par_iter()
                .map(|symbol| {
                    let file_path = path.join(symbol.to_string() + ".feather");
                    try_read_ipc_path(file_path, memory_map, lazy)
                        .unwrap()
                        .ok_or_else(|| anyhow::anyhow!("can not read {} as a feather", &symbol))
                        .unwrap()
                })
                .collect()
        } else {
            fs::read_dir(path)?
                .par_bridge()
                .filter_map(move |file| {
                    let file = file.unwrap();
                    let file_path = file.path();
                    try_read_ipc_path(file_path, memory_map, lazy).unwrap()
                })
                .unzip()
        };
        out.symbols = Some(find_symbols);
        Ok(out.with_dfs(dfs))
    }
}

/// Extracts the file stem from a given path.
///
/// # Arguments
///
/// * `path` - The path from which to extract the file stem.
///
/// # Returns
///
/// Returns an `Option<&str>` containing the file stem if it exists and is valid UTF-8, otherwise `None`.
#[inline]
fn get_file_stem(path: &Path) -> Option<&str> {
    if let Some(stem) = path.file_stem() {
        if let Some(stem_str) = stem.to_str() {
            Some(stem_str)
        } else {
            None
        }
    } else {
        None
    }
}

/// Attempts to read an IPC file from the given path.
///
/// # Arguments
///
/// * `file_path` - The path of the IPC file to read.
/// * `memory_map` - Whether to use memory mapping when reading the file.
/// * `lazy` - Whether to load the data lazily.
///
/// # Returns
///
/// Returns a `Result` containing an `Option` with the file stem and the loaded data frame if successful,
/// otherwise returns an error.
fn try_read_ipc_path<P: AsRef<Path>>(
    file_path: P,
    memory_map: bool,
    lazy: bool,
) -> Result<Option<(Arc<str>, Frame)>> {
    use polars::prelude::*;
    let file_path = file_path.as_ref();
    let file_stem = if let Some(stem) = get_file_stem(file_path) {
        stem.into()
    } else {
        return Ok(None);
    };
    if file_path
        .extension()
        .map(|e| e == "feather")
        .unwrap_or(false)
    {
        if !lazy {
            let file = File::open(file_path)?;
            let mut reader = IpcReader::new(file);
            if memory_map {
                reader = reader.memory_mapped(Some(file_path.to_owned()))
            }
            Ok(Some((file_stem, reader.finish()?.into())))
        } else {
            let args = ScanArgsIpc {
                rechunk: true,
                memory_map,
                ..Default::default()
            };
            Ok(Some((
                file_stem,
                LazyFrame::scan_ipc(file_path, args)?.into(),
            )))
        }
    } else {
        Ok(None)
    }
}
