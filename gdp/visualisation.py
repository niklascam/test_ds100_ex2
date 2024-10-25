import seaborn as sns


def plot_gdp(df):
    """
    This function takes in a dataframe and plots the GDP of the countries in the dataframe.
    """

    return sns.lineplot(data=df, x="Year", y="GDP", hue="Country Name")