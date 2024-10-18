use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyList;
use pyo3_polars::*;
use tea_data_loader::export::tea_strategy::equity::SignalType;
use tea_data_loader::prelude::*;

#[pyclass(name = "DataLoader")]
#[derive(Clone)]
pub struct PyLoader(pub DataLoader);

impl From<DataLoader> for PyLoader {
    #[inline]
    fn from(loader: DataLoader) -> Self {
        PyLoader(loader)
    }
}

#[derive(FromPyObject)]
pub enum PyFrame {
    Eager(PyDataFrame),
    Lazy(PyLazyFrame),
}

impl IntoPy<PyObject> for PyFrame {
    fn into_py(self, py: Python<'_>) -> PyObject {
        match self {
            PyFrame::Eager(df) => df.into_py(py),
            PyFrame::Lazy(lf) => lf.into_py(py),
        }
    }
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
    fn new(typ: &str, symbols: Option<Vec<String>>) -> Self {
        if let Some(symbols) = symbols {
            DataLoader::new_with_symbols(typ, symbols).into()
        } else {
            DataLoader::new(typ).into()
        }
    }

    #[staticmethod]
    fn from_dfs(dfs: Vec<PyFrame>) -> Self {
        let dfs: Vec<Frame> = dfs.into_iter().map(|df| df.into()).collect();
        DataLoader::new_from_dfs(dfs).into()
    }

    fn len(&self) -> usize {
        self.0.len()
    }

    fn __repr__(&self) -> String {
        format!("{:?}", self.0)
    }

    fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    fn is_lazy(&self) -> bool {
        self.0.is_lazy()
    }

    fn is_eager(&self) -> bool {
        !self.0.is_lazy()
    }

    fn with_start(&mut self, start: &str) -> Self {
        let mut out = self.clone();
        out.0 = out.0.with_start(start);
        out
    }

    fn with_end(&mut self, end: &str) -> Self {
        let mut out = self.clone();
        out.0 = out.0.with_end(end);
        out
    }

    fn with_symbols(&mut self, symbols: Vec<String>) -> Self {
        let mut out = self.clone();
        out.0 = out.0.with_symbols(symbols);
        out
    }

    fn with_freq(&mut self, freq: &str) -> Self {
        let mut out = self.clone();
        out.0 = out.0.with_freq(freq);
        out
    }

    #[getter]
    fn get_symbols(&self) -> Option<Vec<&str>> {
        self.0.get_symbols()
    }

    #[getter]
    fn get_dfs<'py>(&'py self, py: Python<'py>) -> Bound<'py, PyList> {
        PyList::new_bound(
            py,
            self.0.dfs.iter().map(|df| match df {
                Frame::Eager(df) => PyDataFrame(df.clone()).into_py(py),
                Frame::Lazy(lf) => PyLazyFrame(lf.clone()).into_py(py),
            }),
        )
    }

    fn __getitem__(&self, obj: &Bound<'_, PyAny>, py: Python<'_>) -> PyObject {
        let idx: usize = obj.extract::<usize>().unwrap();
        let df = self.0.dfs[idx].clone();
        match df {
            Frame::Eager(df) => PyDataFrame(df.clone()).into_py(py),
            Frame::Lazy(lf) => PyLazyFrame(lf.clone()).into_py(py),
        }
    }

    #[getter]
    fn get_type(&self) -> &str {
        &*self.0.typ
    }

    #[getter]
    fn get_freq(&self) -> Option<&str> {
        self.0.freq.as_ref().map(|s| &**s)
    }

    #[setter]
    fn set_dfs(&mut self, dfs: Vec<PyFrame>) {
        let dfs: Vec<Frame> = dfs.into_iter().map(|df| df.into()).collect();
        self.0 = self.0.clone().with_dfs(dfs);
    }

    #[setter]
    fn set_symbols(&mut self, symbols: Vec<String>) {
        self.0 = self.0.clone().with_symbols(symbols);
    }

    #[setter]
    fn set_start(&mut self, start: &str) {
        self.0 = self.0.clone().with_start(start);
    }

    #[setter]
    fn set_end(&mut self, end: &str) {
        self.0 = self.0.clone().with_end(end);
    }

    #[setter]
    fn set_freq(&mut self, freq: &str) {
        self.0 = self.0.clone().with_freq(freq);
    }

    fn with_dfs(&mut self, dfs: Vec<PyFrame>) -> Self {
        let dfs: Vec<Frame> = dfs.into_iter().map(|df| df.into()).collect();
        let mut out = self.clone();
        out.0 = out.0.with_dfs(dfs);
        out
    }

    #[pyo3(signature = (par=true))]
    fn collect(&self, par: bool) -> PyResult<Self> {
        let mut out = self.clone();
        out.0 = out
            .0
            .collect(par)
            .map_err(|e| PyValueError::new_err(e.to_string()))?;
        Ok(out)
    }

    fn lazy(&mut self) {
        self.0 = self.0.clone().lazy();
    }

    #[pyo3(signature = (freq, tier=None, adjust=None, concat_tick_df=false))]
    fn kline(
        &mut self,
        freq: &str,
        tier: Option<&str>,
        adjust: Option<&str>,
        concat_tick_df: bool,
    ) -> PyResult<Self> {
        let mut out = self.clone();
        out.0 = out
            .0
            .kline(KlineOpt {
                freq,
                tier: tier.map(|s| s.parse().unwrap()),
                adjust: adjust.map(|s| s.parse().unwrap()),
                concat_tick_df,
            })
            .map_err(|e| PyValueError::new_err(e.to_string()))?;
        Ok(out)
    }

    #[pyo3(signature = (facs, c_rate=0., c_rate_type="absolute", is_signal=true, init_cash=10000000, bid="bid1", ask="ask1", contract_chg_signal=None, multiplier=None, signal_type="absolute", blowup=false, suffix=""))]
    fn calc_tick_future_ret(
        &mut self,
        facs: Vec<String>,
        c_rate: f64,
        c_rate_type: &str,
        is_signal: bool,
        init_cash: usize,
        bid: &str,
        ask: &str,
        contract_chg_signal: Option<&str>,
        multiplier: Option<f64>,
        signal_type: &str,
        blowup: bool,
        suffix: &str,
    ) -> PyResult<Self> {
        let mut out = self.clone();
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
        let signal_type = match signal_type {
            "absolute" => SignalType::Absolute,
            "percent" => SignalType::Percent,
            _ => {
                return Err(PyValueError::new_err(format!(
                    "Invalid signal_type: {}, must be 'absolute' or 'percent'",
                    signal_type
                )));
            },
        };
        out.0 = out
            .0
            .calc_tick_future_ret(
                &facs,
                &TickFutureRetOpt {
                    c_rate,
                    is_signal,
                    init_cash,
                    bid,
                    ask,
                    contract_chg_signal,
                    multiplier,
                    signal_type,
                    blowup,
                    suffix,
                },
            )
            .map_err(|e| PyValueError::new_err(e.to_string()))?;
        Ok(out)
    }

    fn with_column(&mut self, expr: PyExpr) -> PyResult<Self> {
        let mut out = self.clone();
        out.0 = out
            .0
            .with_column(expr.0)
            .map_err(|e| PyValueError::new_err(e.to_string()))?;
        Ok(out)
    }

    fn with_columns(&mut self, exprs: Vec<PyExpr>) -> PyResult<Self> {
        let mut out = self.clone();
        let exprs: Vec<Expr> = exprs.into_iter().map(|e| e.0).collect();
        out.0 = out
            .0
            .with_columns(exprs)
            .map_err(|e| PyValueError::new_err(e.to_string()))?;
        Ok(out)
    }

    fn select(&mut self, exprs: Vec<PyExpr>) -> PyResult<Self> {
        let mut out = self.clone();
        let exprs: Vec<Expr> = exprs.into_iter().map(|e| e.0).collect();
        out.0 = out
            .0
            .select(exprs)
            .map_err(|e| PyValueError::new_err(e.to_string()))?;
        Ok(out)
    }

    fn filter(&mut self, expr: PyExpr) -> PyResult<Self> {
        let mut out = self.clone();
        out.0 = out
            .0
            .filter(expr.0)
            .map_err(|e| PyValueError::new_err(e.to_string()))?;
        Ok(out)
    }

    fn drop(&mut self, columns: Vec<String>) -> PyResult<Self> {
        let mut out = self.clone();
        out.0 = out
            .0
            .drop(columns)
            .map_err(|e| PyValueError::new_err(e.to_string()))?;
        Ok(out)
    }
}
