import seaborn as sns
import polars as pl
from matplotlib.axes import Axes


def plot_gdp(df: pl.DataFrame) -> Axes:
    """
    This function takes in a dataframe and plots the
    GDP of the countries in the dataframe.
    """

    return sns.lineplot(data=df, x="Year", y="GDP", hue="Country Name")
