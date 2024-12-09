use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::pybacked::PyBackedStr;
use tea_data_loader::export::polars::prelude::*;
use tea_data_loader::export::tea_strategy::equity::SignalType;
use tea_data_loader::export::tevec::agg::CorrMethod;
use tea_data_loader::prelude::Backend;

use super::utils::Wrap;

impl<'py> FromPyObject<'py> for Wrap<JoinType> {
    fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
        let s = ob.extract::<PyBackedStr>()?;
        match &*s {
            "inner" => Ok(Wrap(JoinType::Inner)),
            "left" => Ok(Wrap(JoinType::Left)),
            "outer" => Ok(Wrap(JoinType::Full)),
            "right" => Ok(Wrap(JoinType::Right)),
            "cross" => Ok(Wrap(JoinType::Cross)),
            ty => Err(PyValueError::new_err(format!("Invalid join type: {ty}"))),
        }
    }
}

impl<'py> FromPyObject<'py> for Wrap<SignalType> {
    fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
        let s = ob.extract::<PyBackedStr>()?;
        match &*s {
            "absolute" => Ok(Wrap(SignalType::Absolute)),
            "percent" => Ok(Wrap(SignalType::Percent)),
            signal_type => {
                return Err(PyValueError::new_err(format!(
                    "Invalid signal_type: {}, must be 'absolute' or 'percent'",
                    signal_type
                )));
            },
        }
    }
}

impl<'py> FromPyObject<'py> for Wrap<Label> {
    fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
        let parsed = match &*ob.extract::<PyBackedStr>()? {
            "left" => Label::Left,
            "right" => Label::Right,
            "datapoint" => Label::DataPoint,
            v => {
                return Err(PyValueError::new_err(format!(
                    "`label` must be one of {{'left', 'right', 'datapoint'}}, got {v}",
                )))
            },
        };
        Ok(Wrap(parsed))
    }
}

impl<'py> FromPyObject<'py> for Wrap<ClosedWindow> {
    fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
        let parsed = match &*ob.extract::<PyBackedStr>()? {
            "left" => ClosedWindow::Left,
            "right" => ClosedWindow::Right,
            "both" => ClosedWindow::Both,
            "none" => ClosedWindow::None,
            v => {
                return Err(PyValueError::new_err(format!(
                    "`closed` must be one of {{'left', 'right', 'both', 'none'}}, got {v}",
                )))
            },
        };
        Ok(Wrap(parsed))
    }
}

impl<'py> FromPyObject<'py> for Wrap<StartBy> {
    fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
        let parsed = match &*ob.extract::<PyBackedStr>()? {
            "window" => StartBy::WindowBound,
            "datapoint" => StartBy::DataPoint,
            "monday" => StartBy::Monday,
            "tuesday" => StartBy::Tuesday,
            "wednesday" => StartBy::Wednesday,
            "thursday" => StartBy::Thursday,
            "friday" => StartBy::Friday,
            "saturday" => StartBy::Saturday,
            "sunday" => StartBy::Sunday,
            v => {
                return Err(PyValueError::new_err(format!(
                    "`start_by` must be one of {{'window', 'datapoint', 'monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday'}}, got {v}",
                )))
            }
        };
        Ok(Wrap(parsed))
    }
}

impl<'py> FromPyObject<'py> for Wrap<Backend> {
    fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
        let parsed = match &*ob.extract::<PyBackedStr>()? {
            "polars" | "pl" => Backend::Polars,
            "tevec" | "te" => Backend::Tevec,
            v => {
                return Err(PyValueError::new_err(format!(
                    "`backend` must be one of {{'polars', 'tevec'}}, got {v}",
                )))
            },
        };
        Ok(Wrap(parsed))
    }
}

impl<'py> FromPyObject<'py> for Wrap<CorrMethod> {
    fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
        let parsed = match &*ob.extract::<PyBackedStr>()? {
            "pearson" => CorrMethod::Pearson,
            "spearman" => CorrMethod::Spearman,
            v => {
                return Err(PyValueError::new_err(format!(
                    "`corr_method` must be one of {{'pearson', 'spearman'}}, got {v}",
                )))
            },
        };
        Ok(Wrap(parsed))
    }
}
