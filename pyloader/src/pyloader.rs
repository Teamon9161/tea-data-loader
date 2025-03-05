#![allow(clippy::too_many_arguments)]

use std::borrow::Cow;
use std::path::PathBuf;

use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList, PyType};
use pyo3_polars::*;
use tea_data_loader::export::chrono::NaiveDateTime;
use tea_data_loader::export::polars::prelude::JoinType;
use tea_data_loader::export::tea_strategy::equity::SignalType;
use tea_data_loader::prelude::*;

use crate::utils::{frame_into_py, Wrap};

/// A Python wrapper around the Rust `DataLoader` struct.
///
/// This struct provides a Python interface to the underlying Rust `DataLoader`,
/// allowing Python code to interact with the data loading and processing functionality.
///
/// The struct implements various methods that mirror the Rust `DataLoader` API,
/// with appropriate Python bindings and type conversions.
#[pyclass(name = "DataLoader", subclass)]
#[derive(Clone)]
pub struct PyLoader(pub DataLoader);

impl From<DataLoader> for PyLoader {
    #[inline]
    fn from(loader: DataLoader) -> Self {
        PyLoader(loader)
    }
}

/// An enum representing either an eager or lazy Polars DataFrame in Python.
///
/// This enum provides a way to handle both eager (`PyDataFrame`) and lazy (`PyLazyFrame`)
/// DataFrame types in Python bindings.
///
/// # Variants
///
/// * `Eager` - Contains a `PyDataFrame` representing an eagerly evaluated DataFrame
/// * `Lazy` - Contains a `PyLazyFrame` representing a lazily evaluated DataFrame
#[derive(FromPyObject, IntoPyObject)]
pub enum PyFrame {
    Eager(PyDataFrame),
    Lazy(PyLazyFrame),
}

impl From<PyFrame> for Frame {
    fn from(frame: PyFrame) -> Self {
        match frame {
            PyFrame::Eager(df) => df.0.into(),
            PyFrame::Lazy(lf) => lf.0.into(),
        }
    }
}

impl From<Frame> for PyFrame {
    fn from(frame: Frame) -> Self {
        match frame {
            Frame::Eager(df) => PyFrame::Eager(PyDataFrame(df)),
            Frame::Lazy(lf) => PyFrame::Lazy(PyLazyFrame(lf)),
        }
    }
}

#[pymethods]
impl PyLoader {
    #[new]
    #[pyo3(signature = (typ, symbols=None))]
    /// Creates a new `PyLoader` instance with the specified type and symbols.
    ///
    /// # Arguments
    ///
    /// * `typ` - A string slice that holds the type of data.
    /// * `symbols` - An optional list of symbols.
    ///
    /// # Returns
    ///
    /// A new `PyLoader` instance with the specified type and symbols.
    fn new(typ: &str, symbols: Option<Vec<String>>) -> Self {
        if let Some(symbols) = symbols {
            DataLoader::new_with_symbols(typ, symbols).into()
        } else {
            DataLoader::new(typ).into()
        }
    }

    #[staticmethod]
    /// Creates a new `PyLoader` instance from the provided data frames.
    ///
    /// # Arguments
    ///
    /// * `dfs` - A list of data frames.
    ///
    /// # Returns
    ///
    /// A new `PyLoader` instance with the provided data frames.
    fn from_dfs(dfs: Vec<PyFrame>) -> Self {
        let dfs: Vec<Frame> = dfs.into_iter().map(|df| df.into()).collect();
        DataLoader::new_from_dfs(dfs).into()
    }

    /// Returns the number of data frames in the PyLoader.
    fn __len__(&self) -> usize {
        self.0.len()
    }

    /// Returns the number of data frames in the PyLoader.
    fn len(&self) -> usize {
        self.0.len()
    }

    fn __repr__(&self) -> String {
        format!("{:#?}", self.0)
    }

    /// Returns the schema of the first data frame in the `DataLoader`.
    ///
    /// # Returns
    ///
    /// A `Result` containing the `SchemaRef` of the first data frame or an error.
    /// If the `DataLoader` is empty, returns an empty `SchemaRef`.
    fn schema<'py>(&'py self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        PySchema(self.0.schema()?).into_pyobject(py)
    }

    /// Returns a list of column names from the first data frame in the `PyLoader`.
    ///
    /// # Returns
    ///
    /// A `PyResult` containing a vector of column names as `PlSmallStr`.
    /// If there is an error getting the schema, returns the error wrapped in `PyResult`.
    fn columns(&self) -> PyResult<Vec<String>> {
        Ok(self
            .0
            .schema()?
            .iter_names()
            .map(|s| s.to_string())
            .collect())
    }

    /// Returns `true` if the `PyLoader` contains no data frames.
    fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Checks if the `PyLoader` is lazy.
    ///
    /// This method determines if the data loading is lazy by checking the first data frame.
    /// If the `PyLoader` is empty, it is considered not lazy.
    ///
    /// # Returns
    ///
    /// `true` if the `PyLoader` is lazy, `false` otherwise.
    fn is_lazy(&self) -> bool {
        self.0.is_lazy()
    }

    /// Returns `true` if the `PyLoader` is eager.
    fn is_eager(&self) -> bool {
        !self.0.is_lazy()
    }

    /// Finds the index of a given symbol in the PyLoader's symbols list.
    ///
    /// # Arguments
    ///
    /// * `symbol` - The symbol name to search for
    ///
    /// # Returns
    ///
    /// Returns `Some(index)` if the symbol is found, where `index` is the position
    /// of the symbol in the list. Returns `None` if either:
    /// - The symbol is not found
    /// - The PyLoader has no symbols list
    #[inline]
    fn find_index(&self, symbol: &str) -> Option<usize> {
        self.0.find_index(symbol)
    }

    /// Sets the type for the `PyLoader`.
    ///
    /// # Arguments
    ///
    /// * `typ` - Any type that can be referenced as a str.
    ///
    /// # Returns
    ///
    /// The modified `PyLoader` instance.
    fn with_type(&mut self, typ: &str) -> Self {
        self.0.clone().with_type(typ).into()
    }

    /// Sets the start date/time for the `PyLoader`.
    ///
    /// # Arguments
    ///
    /// * `start` - A string representing the start date/time.
    ///
    /// # Returns
    ///
    /// The modified `PyLoader` instance.
    fn with_start(&mut self, start: &str) -> Self {
        self.0.clone().with_start(start).into()
    }

    /// Sets the end date/time for the `PyLoader`.
    ///
    /// # Arguments
    ///
    /// * `end` - A string representing the end date/time.
    ///
    /// # Returns
    ///
    /// The modified `PyLoader` instance.
    fn with_end(&mut self, end: &str) -> Self {
        self.0.clone().with_end(end).into()
    }

    /// Sets the symbols for the `PyLoader`.
    ///
    /// # Arguments
    ///
    /// * `symbols` - A list of symbols.
    ///
    /// # Returns
    ///
    /// The modified `PyLoader` instance.
    fn with_symbols(&mut self, symbols: Vec<String>) -> Self {
        self.0.clone().with_symbols(symbols).into()
    }

    /// Sets the frequency for the `PyLoader`.
    ///
    /// # Arguments
    ///
    /// * `freq` - A string representing the frequency.
    ///
    /// # Returns
    ///
    /// The modified `PyLoader` instance.
    fn with_freq(&mut self, freq: &str) -> Self {
        self.0.clone().with_freq(freq).into()
    }

    #[getter]
    /// Returns the start date/time if present in the `PyLoader`.
    ///
    /// # Returns
    ///
    /// An optional string representing the start date/time.
    fn get_start(&self) -> Option<NaiveDateTime> {
        self.0.start.map(|d| d.as_cr().unwrap().naive_utc())
    }

    #[getter]
    /// Returns the end date/time if present in the `PyLoader`.
    ///
    /// # Returns
    ///
    /// An optional string representing the end date/time.
    fn get_end(&self) -> Option<NaiveDateTime> {
        self.0.end.map(|d| d.as_cr().unwrap().naive_utc())
    }
    #[getter]
    /// Returns a list of symbols if present in the `PyLoader`.
    ///
    /// # Returns
    ///
    /// An optional list of symbols.
    fn get_symbols(&self) -> Option<Vec<&str>> {
        self.0.get_symbols()
    }

    #[getter]
    /// Returns the list of data frames.
    fn get_dfs<'py>(&'py self, py: Python<'py>) -> PyResult<Bound<'py, PyList>> {
        PyList::new(
            py,
            self.0
                .dfs
                .iter()
                .map(|df| frame_into_py(df.clone(), py).unwrap()),
        )
    }

    fn __getitem__<'py>(
        &'py self,
        obj: &Bound<'py, PyAny>,
        py: Python<'py>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let df = if let Ok(idx) = obj.extract::<Cow<'_, str>>() {
            self.0[idx.as_ref()].clone()
        } else {
            let idx: usize = obj.extract::<usize>().unwrap();
            self.0.dfs[idx].clone()
        };
        frame_into_py(df, py)
    }

    fn __setitem__(&mut self, idx: &Bound<'_, PyAny>, value: PyFrame) -> PyResult<()> {
        if let Ok(idx) = idx.extract::<Cow<'_, str>>() {
            self.0.insert(idx.as_ref(), value)?;
        } else {
            let idx: usize = idx.extract::<usize>().unwrap();
            self.0.dfs[idx] = value.into();
        }
        Ok(())
    }

    #[getter]
    /// Returns the type of data, such as future, bond, stock, etc.
    fn get_type(&self) -> &str {
        &self.0.typ
    }

    #[getter]
    /// Returns the frequency of the data, such as "1d" for daily, "1h" for hourly, etc.
    fn get_freq(&self) -> Option<&str> {
        self.0.freq.as_deref()
    }

    #[setter]
    /// Sets the list of data frames.
    fn set_dfs(&mut self, dfs: Vec<PyFrame>) {
        let dfs: Vec<Frame> = dfs.into_iter().map(|df| df.into()).collect();
        self.0 = self.0.clone().with_dfs(dfs);
    }

    #[setter]
    /// Sets the symbols for the `PyLoader`.
    fn set_symbols(&mut self, symbols: Vec<String>) {
        self.0 = self.0.clone().with_symbols(symbols);
    }

    #[setter]
    /// Sets the type of data, such as future, bond, stock, etc.
    fn set_type(&mut self, typ: &str) {
        self.0 = self.0.clone().with_type(typ);
    }

    #[setter]
    /// Sets the start date/time for the `PyLoader`.
    fn set_start(&mut self, start: &str) {
        self.0 = self.0.clone().with_start(start);
    }

    #[setter]
    /// Sets the end date/time for the `PyLoader`.
    fn set_end(&mut self, end: &str) {
        self.0 = self.0.clone().with_end(end);
    }

    #[setter]
    /// Sets the frequency for the `PyLoader`.
    fn set_freq(&mut self, freq: &str) {
        self.0 = self.0.clone().with_freq(freq);
    }

    /// Sets the data frames for the `PyLoader`.
    ///
    /// # Arguments
    ///
    /// * `dfs` - A list of data frames.
    ///
    /// # Returns
    ///
    /// The modified `PyLoader` instance.
    fn with_dfs(&mut self, dfs: Vec<PyFrame>) -> Self {
        let dfs: Vec<Frame> = dfs.into_iter().map(|df| df.into()).collect();
        let mut out = self.clone();
        out.0 = out.0.with_dfs(dfs);
        out
    }

    #[pyo3(signature = (par=true, inplace=false))]
    /// Collects the data frames in the `PyLoader`.
    ///
    /// # Arguments
    ///
    /// * `par` - A boolean indicating whether to use parallel processing.
    ///
    /// # Returns
    ///
    /// A `PyResult` containing the modified `PyLoader` instance or an error.
    fn collect(
        mut slf: PyRefMut<'_, Self>,
        par: bool,
        inplace: bool,
    ) -> PyResult<Bound<'_, PyLoader>> {
        let py = slf.py();
        if inplace {
            slf.0 = slf.0.clone().collect(par)?;
            Ok(slf.into_pyobject(py).unwrap())
        } else {
            let mut out = slf.clone();
            out.0 = out.0.collect(par)?;
            out.into_pyobject(py)
        }
    }

    /// Converts the data frames in the `PyLoader` to lazy frames.
    ///
    /// # Returns
    ///
    /// The modified `PyLoader` instance with lazy frames.
    fn lazy(&self) -> Self {
        self.0.clone().lazy().into()
    }

    #[pyo3(signature = (freq, tier=None, adjust=None, concat_tick_df=false))]
    /// Loads kline data based on the given options.
    ///
    /// # Arguments
    ///
    /// * `freq` - The frequency of the kline data.
    /// * `tier` - Optional tier level.
    /// * `adjust` - Optional adjustment type.
    /// * `concat_tick_df` - Whether to concatenate tick data frames.
    ///
    /// # Returns
    ///
    /// A `PyResult` containing the modified `PyLoader` instance or an error.
    fn kline(
        &self,
        freq: &str,
        tier: Option<&str>,
        adjust: Option<&str>,
        concat_tick_df: bool,
    ) -> PyResult<Self> {
        let out = self.0.clone().kline(KlineOpt {
            freq,
            tier: tier.map(|s| s.parse().unwrap()),
            adjust: adjust.map(|s| s.parse().unwrap()),
            concat_tick_df,
        })?;
        Ok(out.into())
    }

    #[pyo3(signature = (facs, c_rate=0., c_rate_type="absolute", is_signal=true, init_cash=10000000, bid="bid1", ask="ask1", contract_chg_signal=None, multiplier=None, signal_type=Wrap(SignalType::Absolute), blowup=false, suffix=""))]
    /// Calculates tick-based future returns for the given factors.
    ///
    /// # Arguments
    ///
    /// * `facs` - List of factor names.
    /// * `c_rate` - Commission rate.
    /// * `c_rate_type` - Commission rate type ("absolute" or "percent").
    /// * `is_signal` - Whether the factors are signals.
    /// * `init_cash` - Initial cash amount.
    /// * `bid` - Bid price column name.
    /// * `ask` - Ask price column name.
    /// * `contract_chg_signal` - Optional contract change signal column.
    /// * `multiplier` - Optional contract multiplier.
    /// * `signal_type` - Signal type ("absolute" or "percent").
    /// * `blowup` - Whether to allow blowup.
    /// * `suffix` - Suffix for output columns.
    ///
    /// # Returns
    ///
    /// A `PyResult` containing the modified `PyLoader` instance or an error.
    fn calc_tick_future_ret(
        &self,
        facs: Vec<String>,
        c_rate: f64,
        c_rate_type: &str,
        is_signal: bool,
        init_cash: usize,
        bid: &str,
        ask: &str,
        contract_chg_signal: Option<&str>,
        multiplier: Option<f64>,
        signal_type: Wrap<SignalType>,
        blowup: bool,
        suffix: &str,
    ) -> PyResult<Self> {
        let c_rate = match c_rate_type {
            "absolute" => CRate::Absolute(c_rate),
            "relative" | "percent" => CRate::Percent(c_rate),
            _ => {
                return Err(PyValueError::new_err(format!(
                    "Invalid c_rate_type: {}, must be 'absolute' or 'percent'",
                    c_rate_type
                )));
            },
        };
        let out = self.0.clone().calc_tick_future_ret(
            &facs,
            &TickFutureRetOpt {
                c_rate,
                is_signal,
                init_cash,
                bid,
                ask,
                contract_chg_signal,
                multiplier,
                signal_type: signal_type.0,
                blowup,
                suffix,
            },
        )?;
        Ok(out.into())
    }

    // /// Adds a new column to each DataFrame in the PyLoader.
    // ///
    // /// # Arguments
    // ///
    // /// * `expr` - The expression defining the new column.
    // ///
    // /// # Returns
    // ///
    // /// A `PyResult` containing the modified `PyLoader` instance or an error.
    // fn with_column(&self, expr: PyExpr) -> PyResult<Self> {
    //     Ok(self.0.clone().with_column(expr.0)?.into())
    // }

    /// Adds multiple new columns to each DataFrame in the PyLoader.
    ///
    /// # Arguments
    ///
    /// * `exprs` - A list of expressions defining the new columns.
    ///
    /// # Returns
    ///
    /// A `PyResult` containing the modified `PyLoader` instance or an error.
    fn with_columns(&self, exprs: Vec<PyExpr>) -> PyResult<Self> {
        let exprs: Vec<Expr> = exprs.into_iter().map(|e| e.0).collect();
        Ok(self.0.clone().with_columns(exprs)?.into())
    }

    /// Selects specific columns from each DataFrame in the PyLoader.
    ///
    /// # Arguments
    ///
    /// * `exprs` - A list of expressions defining the columns to select.
    ///
    /// # Returns
    ///
    /// A `PyResult` containing the modified `PyLoader` instance or an error.
    fn select(&self, exprs: Vec<PyExpr>) -> PyResult<Self> {
        let exprs: Vec<Expr> = exprs.into_iter().map(|e| e.0).collect();
        Ok(self.0.clone().select(exprs)?.into())
    }

    /// Filters rows in each DataFrame of the PyLoader based on a given expression.
    ///
    /// # Arguments
    ///
    /// * `expr` - The filter expression.
    ///
    /// # Returns
    ///
    /// A `PyResult` containing the modified `PyLoader` instance or an error.
    fn filter(&self, expr: PyExpr) -> PyResult<Self> {
        Ok(self.0.clone().filter(expr.0)?.into())
    }

    /// Drops specified columns from each DataFrame in the PyLoader.
    ///
    /// # Arguments
    ///
    /// * `columns` - A list of expressions specifying the columns to drop.
    /// * `strict` - If true, raises an error if any specified column doesn't exist.
    ///             If false, silently ignores non-existent columns.
    ///
    /// # Returns
    ///
    /// A `PyResult` containing the modified `PyLoader` instance or an error.
    #[pyo3(signature = (columns, strict=false))]
    fn drop(&self, columns: Vec<PyExpr>, strict: bool) -> PyResult<Self> {
        let columns: Vec<Expr> = columns.into_iter().map(|e| e.0).collect();
        if strict {
            Ok(self.0.clone().drop_strict(columns)?.into())
        } else {
            Ok(self.0.clone().drop(columns)?.into())
        }
    }

    /// Aligns multiple DataFrames based on specified columns and join type.
    ///
    /// This method aligns the DataFrames in the `PyLoader` by performing a series of joins
    /// on the specified columns. It creates a master alignment frame and then extracts
    /// individual aligned frames from it.
    ///
    /// # Arguments
    ///
    /// * `on` - An expression or slice of expressions specifying the columns to align on.
    /// * `how` - An optional `JoinType` specifying the type of join to perform. Defaults to `JoinType::Full` if not provided.
    ///
    /// # Returns
    ///
    /// A `PyResult` containing the modified `PyLoader` instance or an error.
    ///
    /// # Notes
    ///
    /// - If the `PyLoader` is empty, it returns the original instance.
    /// - For large numbers of frames (more than `POST_ALIGN_COLLECT_NUM`), it may need to collect eagerly to avoid stack overflow.
    /// - The method sorts the resulting frames based on the alignment columns.
    #[pyo3(signature=(on, how=None))]
    fn align(&self, on: Vec<PyExpr>, how: Option<Wrap<JoinType>>) -> PyResult<Self> {
        let on: Vec<Expr> = on.into_iter().map(|e| e.0).collect();
        Ok(self.0.clone().align(on, how.map(|h| h.0))?.into())
    }

    /// Saves the `DataLoader` data to a file or directory.
    ///
    /// # Arguments
    ///
    /// * `path` - The path where the data should be saved. Can be a file path with extension or a directory path.
    ///            If a directory path is provided, data will be saved in IPC (Arrow IPC) format.
    ///
    /// # Returns
    ///
    /// Returns `PyResult<()>` if the save operation is successful, otherwise returns an error.
    fn save(&self, path: PathBuf) -> PyResult<()> {
        self.0.save(path)?;
        Ok(())
    }

    #[classmethod]
    #[pyo3(signature = (path, symbols=None, lazy=true))]
    /// Loads data from a `DataLoader` file or directory.
    ///
    /// # Arguments
    ///
    /// * `path` - The path from where the data should be loaded. Can be a file path or directory path.
    /// * `symbols` - Optional list of symbols to load. If provided, only loads data for these symbols.
    ///              Only applicable when loading from a directory.
    /// * `lazy` - Whether to load the data lazily. Defaults to true.
    ///
    /// # Returns
    ///
    /// Returns `PyResult<PyLoader>` containing the loaded data if successful, otherwise returns an error.
    fn load(
        _cls: &Bound<'_, PyType>,
        path: PathBuf,
        symbols: Option<Vec<String>>,
        lazy: bool,
    ) -> PyResult<Self> {
        let loader = if let Some(symbols) = symbols {
            DataLoader::load_symbols(path, &symbols, lazy)?
        } else {
            DataLoader::load(path, lazy)?
        };
        Ok(PyLoader(loader))
    }

    /// Concatenates all DataFrames in the DataLoader into a single LazyFrame.
    ///
    /// This method performs the following operations:
    /// 1. Iterates through all DataFrames in the DataLoader
    /// 2. For each DataFrame, it checks if a 'symbol' column exists
    /// 3. If 'symbol' column doesn't exist, it adds one using the symbol associated with the DataFrame
    /// 4. Converts each DataFrame to a LazyFrame
    /// 5. Concatenates all LazyFrames vertically
    ///
    /// # Returns
    ///
    /// Returns a `PyResult<LazyFrame>` which is the concatenated LazyFrame of all DataFrames in the DataLoader.
    ///
    /// # Errors
    ///
    /// This function will return an error if there are issues with DataFrame operations or concatenation.
    fn concat(&self) -> PyResult<PyLazyFrame> {
        Ok(PyLazyFrame(self.0.clone().concat()?))
    }

    #[pyo3(signature = (path, on=None, left_on=None, right_on=None, how=Wrap(JoinType::Left), flag=true))]
    /// Joins the current DataLoader with another dataset.
    ///
    /// This method performs a join operation between the current DataLoader and another dataset.
    /// It supports various join types and options.
    ///
    /// # Arguments
    ///
    /// * `path` - The path to the other dataset.
    /// * `on` - Optional columns to join on for both datasets. Cannot be used with left_on/right_on.
    /// * `left_on` - Optional columns to join on from the left (current) dataset. Required if `on` not provided.
    /// * `right_on` - Optional columns to join on from the right (other) dataset. Required if `on` not provided.
    /// * `how` - The type of join to perform (left, right, inner, outer). Defaults to left join.
    /// * `flag` - Whether to perform the join operation. Defaults to true.
    ///
    /// # Returns
    ///
    /// Returns `PyResult<PyLoader>` containing the joined data if successful, otherwise returns an error.
    fn join(
        &self,
        path: PathBuf,
        on: Option<Vec<PyExpr>>,
        left_on: Option<Vec<PyExpr>>,
        right_on: Option<Vec<PyExpr>>,
        how: Wrap<JoinType>,
        flag: bool,
    ) -> PyResult<Self> {
        if let Some(on) = on {
            if left_on.is_some() || right_on.is_some() {
                return Err(PyValueError::new_err(
                    "on and left_on/right_on cannot be used together",
                ));
            }
            let on: Vec<Expr> = on.into_iter().map(|e| e.0).collect();
            let join_opt = JoinOpt::new_on(path, &on, how.0, flag);
            Ok(PyLoader(self.0.clone().join(join_opt)?))
        } else {
            let left_on: Vec<Expr> = left_on
                .expect("left_on is required")
                .into_iter()
                .map(|e| e.0)
                .collect();
            let right_on: Vec<Expr> = right_on
                .expect("right_on is required")
                .into_iter()
                .map(|e| e.0)
                .collect();
            let join_opt = JoinOpt::new(path, left_on, right_on, how.0, flag);
            Ok(PyLoader(self.0.clone().join(join_opt)?))
        }
    }

    /// Applies a Python function to each DataFrame in the DataLoader.
    ///
    /// # Arguments
    ///
    /// * `func` - A Python callable that takes a DataFrame as input and returns a DataFrame
    /// * `kwargs` - Optional keyword arguments to pass to the Python function
    ///
    /// # Returns
    ///
    /// Returns `PyResult<PyLoader>` containing the transformed DataLoader if successful, otherwise returns an error.
    #[pyo3(signature = (func, **kwargs))]
    fn apply(&self, func: &Bound<'_, PyAny>, kwargs: Option<&Bound<'_, PyDict>>) -> PyResult<Self> {
        let dl = self.0.clone().try_apply(|df| {
            let pydf: PyFrame = df.into();
            let result = func.call((pydf,), kwargs)?;
            let df = result.extract::<PyFrame>()?;
            Ok(df.into())
        })?;
        Ok(PyLoader(dl))
    }
}
