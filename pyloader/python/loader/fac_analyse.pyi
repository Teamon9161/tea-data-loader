from typing import overload
from polars import DataFrame, Series
from .py_loader import DataLoader
from typing import Literal

class Summary:
    """A summary of factor analysis results containing various metrics and calculations."""
    
    @property
    def facs(self) -> list[str]:
        """Get the list of factor names."""
    
    @property
    def labels(self) -> list[str]:
        """Get the list of label names."""
    
    @property
    def symbol_ic(self) -> list[DataLoader]:
        """Get the symbol-level IC for each factor.
        Each element is a DataLoader containing IC values for different symbols for one factor."""
    
    @property
    def ic_overall(self) -> list[DataFrame]:
        """Get the overall IC for each factor."""
    
    @property
    def ts_ic(self) -> list[DataFrame]:
        """Get the time-series IC for each factor.
        Each DataFrame is a factor's time-series IC, with columns representing IC for different labels."""
    
    @property
    def symbol_ts_group_rets(self) -> list[DataLoader]:
        """Get the symbol-level time-series group returns for each factor."""
    
    @property
    def ts_group_rets(self) -> list[DataFrame]:
        """Get the time-series group returns for each factor.
        Returns group returns calculated over time periods, averaged to generate group performance curves."""
    
    @property
    def symbol_group_rets(self) -> list[DataLoader]:
        """Get the symbol-level group returns for each factor.
        Returns average returns for each group per factor, before averaging across symbols."""
    
    @property
    def group_rets(self) -> list[DataFrame]:
        """Get the group returns for each factor.
        Returns average returns for each group."""
    
    @property
    def half_life(self) -> DataFrame | None:
        """Get the half-life values for each factor.
        Returns half-life calculations for factors if available."""
    
    def finish(self) -> "SummaryReport":
        """Finalize the summary and create a SummaryReport."""

class FacSummary:
    """Summary statistics and analysis results for a single factor."""
    
    @property
    def fac(self) -> str:
        """Get the factor name."""
    
    @property
    def labels(self) -> list[str]:
        """Get the list of label names."""
    
    @property
    def symbol_ic(self) -> DataLoader | None:
        """Get the symbol-level IC for the factor."""
    
    @property
    def ic_overall(self) -> DataFrame | None:
        """Get the overall IC for the factor."""
    
    @property
    def ts_ic(self) -> DataFrame | None:
        """Get the time-series IC for the factor."""
    
    @property
    def symbol_ts_group_rets(self) -> DataLoader | None:
        """Get the symbol-level time-series group returns for the factor."""
    
    @property
    def ts_group_rets(self) -> DataFrame | None:
        """Get the time-series group returns for the factor."""
    
    @property
    def symbol_group_rets(self) -> DataLoader | None:
        """Get the symbol-level group returns for the factor."""
    
    @property
    def group_rets(self) -> DataFrame | None:
        """Get the group returns for the factor."""
    
    @property
    def half_life(self) -> float | None:
        """Get the half-life for the factor."""

class FacAnalysis:
    """A class for performing factor analysis with various metrics and calculations."""
    
    @property
    def summary(self) -> Summary:
        """Get a Summary object that contains overall results of the analysis."""
    
    def with_ic_overall(self, method: Literal["pearson"] = "pearson") -> "FacAnalysis":
        """Calculate overall Information Coefficient (IC) for the factors."""
    
    def with_ts_ic(self, rule: str, method: Literal["pearson"] = "pearson") -> "FacAnalysis":
        """Calculate time-series Information Coefficient (IC) for the factors."""
    
    def with_ts_group_ret(self, group: int = 10) -> "FacAnalysis":
        """Calculate time-series group returns."""
    
    def with_group_ret(self, rule: str | None = None, group: int = 10) -> "FacAnalysis":
        """Calculate group returns."""
    
    def with_half_life(self) -> "FacAnalysis":
        """Calculate half-life values for the factors."""

class SummaryReport:
    """A comprehensive report containing analysis results for multiple factors."""
    
    @overload
    def __getitem__(self, index: str) -> FacSummary:
        """Get a FacSummary by factor name."""
    @overload
    def __getitem__(self, index: int) -> FacSummary:
        """Get a FacSummary by index."""
    
    @property
    def labels(self) -> list[str]:
        """Get the list of label names."""
    
    @property
    def fac_series(self) -> Series:
        """Get the factor series."""
    
    @property
    def ts_ic(self) -> list[DataFrame]:
        """Get the time-series IC for each factor."""
    
    @property
    def ic(self) -> DataFrame:
        """Get the IC (Information Coefficient) for each factor."""
    
    @property
    def ir(self) -> DataFrame:
        """Get the IR (Information Ratio) for each factor."""
    
    @property
    def ic_std(self) -> DataFrame:
        """Get the standard deviation of IC for each factor."""
    
    @property
    def ic_skew(self) -> DataFrame:
        """Get the skewness of IC for each factor."""
    
    @property
    def ic_kurt(self) -> DataFrame:
        """Get the kurtosis of IC for each factor."""
    
    @property
    def ic_overall(self) -> DataFrame:
        """Get the overall IC for each factor."""
    
    @property
    def group_rets(self) -> list[DataFrame]:
        """Get the group returns for each factor."""
    
    @property
    def half_life(self) -> DataFrame:
        """Get the half-life for each factor."""
