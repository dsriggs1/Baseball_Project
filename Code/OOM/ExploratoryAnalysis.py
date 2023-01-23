from typing import Callable, Optional

import matplotlib.pyplot as plt
import polars as pl

from Code.Production.create_model_ready import Database


class ExploratoryAnalysis(Database):
    def __init__(self, uid, pwd, host, db, port, df):
        super().__init__(uid, pwd, host, db, port)
        self.df = df


class Plotting(ExploratoryAnalysis):
    def __init__(self, df, uid, pwd, host, db, port):
        super().__init__(uid, pwd, host, db, port, df)

    """
    Initialize the Plotting class by calling the super class's constructor.
    :param df: DataFrame to be plotted
    :param uid: User id for database connection
    :param pwd: Password for database connection
    :param host: Host for database connection
    :param db: Database for connection
    :param port: Port for database connection
    """

    def scatterplot(self, x: str, y: str, i: int = None, axs: object = None, marker: str = None, xlabel: str = None,
                    ylabel: str = None, title: str = None, color: str = None,
                    edgecolor: str = None, alpha: float = None, label: str = "Legend", loc: str = "best"):
        """
        Create a scatter plot using the given parameters.
        :param x: x-axis column name
        :param y: y-axis column name
        :param i: index of the subplot
        :param axs: axis of the subplot
        :param marker: marker style for scatter plot
        :param xlabel: label for x-axis
        :param ylabel: label for y-axis
        :param title: title of the plot
        :param color: color of the points
        :param edgecolor: edge color of the points
        :param alpha: transparency of the points
        :param label: label for legend
        :param loc: location of the legend

        :Returns: Scatterplot
        """
        if axs is None:
            plt.scatter(self.df.select(x).collect(), self.df.select(y).collect(), marker=marker, color=color,
                        edgecolor=edgecolor, alpha=alpha)
            plt.legend()
            plt.legend(loc=loc)

        else:
            axs[i].hist(self.df.select(x).collect(), color=color)
            axs[i].set_title(title)
        plt.xlabel(xlabel if xlabel else xlabel)
        plt.ylabel(ylabel if ylabel else ylabel)

    def histogram(self, x: str, y: str = None, i: int = None, axs: object = None, xlabel: str = None,
                  ylabel: str = None, bins: int = None, title: str = None, color: str = None,
                  label: str = "Legend", loc: str = "best"):
        """
        Create a histogram of the given column.
        :param x: column name for histogram
        :param y: column name for y-axis (if creating a 2D histogram)
        :param i: index of the subplot
        :param axs: axis of the subplot
        :param xlabel: label for x-axis
        :param ylabel: label for y-axis
        :param bins: number of bins for histogram
        :param title: title of the plot
        :param color: color of the bars
        :param label: label for legend
        :param loc: location of the legend

        :Returns: Histogram
        """
        if axs is None:
            plt.hist(self.df.select(x).collect(), bins=bins, color=None, edgecolor=None, alpha=None)
            plt.legend()
            plt.legend(loc=loc)

        else:
            axs[i].hist(self.df.select(x).collect(), color=color)
            axs[i].set_title(title)
        plt.xlabel(xlabel)
        plt.ylabel(ylabel)

    def heatmap(self):
        # Method for plotting heatmaps
        pass

    def barplot(self, x, y, agg, xlabel=None, ylabel=None, xlabelsize=None, rotation=45):
        out = (
            self.df.lazy()
            .groupby(y)
            .agg(
                [
                    pl.sum(x).alias("sum"),
                    pl.mean(x).alias("mean"),
                    pl.median(x).alias("median"),
                    pl.max(x).alias("max"),
                    pl.min(x).alias("min")
                ]
            )
        ).collect()
        plt.xlabel(xlabel)
        plt.ylabel(ylabel)
        plt.bar(out[y], out[agg], color=None, edgecolor=None, alpha=None)
        plt.xticks(rotation=rotation)
        plt.tick_params(axis='x', labelsize=xlabelsize)

    def boxplot(self):
        # Method for plotting boxplots
        pass

    def timeseries(self, x, y, xlabel, ylabel, marker, alpha, linewidth, linestyle):
        plt.plot(self.df.select(x).collect(), self.df.select(y).collect(), marker, alpha,
                 linewidth=linewidth,
                 linestyle=linestyle)
        plt.xlabel(xlabel)
        plt.ylabel(ylabel)

    def combine_plots(self, plot_functions: list[Callable], x_values: list[any], y_values: list[any],
                      title_values: list[str], nrows: int = 2, ncols: int = 1, color_values: list[str] = None,
                      legend: str = None, loc: str = None, figsize: tuple[int, int] = (10, 4)):
        """
        This function takes in a list of plot functions, x and y values, title values, and other optional parameters to generate multiple plots in one figure.

        Parameters:
        - plot_functions (List[Callable]): List of functions to generate plots
        - x_values (List[Any]): List of x values for each plot
        - y_values (List[Any]): List of y values for each plot
        - title_values (List[str]): List of title values for each plot
        - nrows (int): Number of rows in the figure. Default is 2
        - ncols (int): Number of columns in the figure. Default is 1
        - color_values (List[str]): List of color values for each plot. Default is None
        - legend (str): The legend for the plot. Default is None
        - loc (str): The location of the legend. Default is None
        - figsize (Tuple[int, int]): The width and height of the figure. Default is (10, 4)

        Returns:
        Page of plots
        """
        fig, axs = plt.subplots(nrows, ncols, figsize=figsize)

        for i, func in enumerate(plot_functions):
            x = x_values[i]
            y = y_values[i] if y_values else None
            color = color_values[i] if color_values else None
            title = title_values[i] if title_values else None
            func(x=x, y=y, df=self.df, i=i, axs=axs, title=title, color=color)
        plt.legend([legend], loc=loc)
        plt.show()

    class VariableManipulation(ExploratoryAnalysis):
        def __init__(self, df, uid, pwd, host, db, port, var_list):
            super().__init__(df, uid, pwd, host, db, port)
            self.var_list = var_list

        def check_var_type(self):
            """
                Print the data type of a variable in a DataFrame.

                Parameters:
                    df (polars.DataFrame): The DataFrame containing the variable.
                    var_list (str): List of variables to check.

                Returns:
                    None
                """
            list(map(lambda var: print(self.df[var].dtype), self.var_list))

        def change_var_type(self):
            # Method for converting variable types
            pass

        def z_score_scaling(self):
            """Standardize a given list of columns in a dataframe.

            Parameters:
                df (polar.DataFrame): The dataframe to be standardized.
                var_list (list): A list of column names in the dataframe to be standardized.

            Returns:
                polar.DataFrame: The input dataframe with the specified columns standardized. The column names are suffixed with "_standard".
            """
            out = self.df.lazy().with_columns([
                ((pl.col(self.var_list) - pl.col(self.var_list).mean())
                 / pl.col(self.var_list).std()).suffix("_zscore")
            ])
            return out

        def min_max_scaling(self):
            out = self.df.lazy().with_columns([
                ((pl.col(self.var_list) - pl.col(self.var_list).min())
                 / (pl.col(self.var_list).max() - pl.col(self.var_list).min())).suffix("_minmax")
            ])
            return out

        def range_scaling(self):
            out = self.df.lazy().with_columns([
                ((pl.col(self.var_list)
                  / (pl.col(self.var_list).max() - pl.col(self.var_list).min()))).suffix("_range")
            ])
            return out

        def std_scaling(self):
            out = self.df.lazy().with_columns([
                (pl.col(self.var_list) / pl.col(self.var_list).std()).suffix("_std")
            ])
            return out

        def transform_vars(self, numpy_func: Callable, var_suffix: str) -> pl.DataFrame:
            """
            Applies a numpy transformation to a list of variables in a polars dataframe and adds a suffix to the resulting columns.

            Parameters:
                - df (pl.DataFrame): The dataframe to apply the transformation to.
                - var_list (List[str]): A list of strings representing the variables to transform.
                - numpy_func (Callable): A numpy function to apply to the variables.
                - var_suffix (str): A string to append as suffix to the transformed variable names.

            Returns:
                - pl.DataFrame: A copy of the input dataframe with the transformed variables.
            """
            out = self.df.lazy().with_columns(numpy_func(pl.col(self.var_list)).suffix(var_suffix))
            return out

        def create_interaction(self):
            # Method for creating interaction variables
            pass

        def create_yeo_johnson(self):
            # Method for creating yeo-johnson variables
            pass

        def fill_missing_values(self, column: str, strategy: Optional[str] = None, interpolation: Optional[str] = None,
                                expression: Optional[str] = None) -> pl.DataFrame:
            """
            Fill missing values in a column either by strategy, interpolation, or expression.
            Valid strategies are None, ‘forward’, ‘backward’, ‘min’, ‘max’, ‘mean’, ‘zero’, ‘one’

            :param column: Column to fill missing values
            :param strategy: Strategy to fill missing values

            :return: Polars DataFrame with missing values filled
            """
            non_none_count = sum([1 for x in (strategy, interpolation, expression) if x is not None])
            if non_none_count > 1:
                raise ValueError("Only one of strategy, interpolation, and expression can be set.")

            if strategy != None:
                df = df.with_column(
                    pl.col(column).fill_null(strategy=strategy),
                )
            elif interpolation != None:
                df = df.with_column(
                    pl.col(column).interpolate(),
                )
            else:
                df = df.with_column(
                    pl.col(column).fill_null(expression),
                )

            return df

        def binning(self):
            # Method for binning variables
            pass

        def one_hot_encoding(self):
            # Method for one-hot encoding variables
            pass

        def label_encoding(self):
            # Method for label encoding variables
            pass


class SummarizeData(ExploratoryAnalysis):
    def __init__(self, var_list, df, uid, pwd, host, db, port):
        super().__init__(df, uid, pwd, host, db, port)
        self.var_list = var_list

    def pearson_corr(self):
        # Method for calculating pearson correlations
        corr_df = self.df.select([
            pl.col(self.var_list)
        ]).collect()
        return corr_df.pearson_corr()

    def spearman_corr(self):
        corr_df = self.df.select([
            pl.col(self.var_list)
        ]).collect().to_pandas().corr(method="spearman")
        return corr_df

    def categorical_summary(self, df, var, groupby_var):
        # method for summarizing categorical variables
        out = (
            df.lazy()
            .groupby(groupby_var)
            .agg(
                [
                    pl.sum(var).alias("sum"),
                    pl.mean(var).alias("mean"),
                    pl.median(var).alias("median"),
                    pl.max(var).alias("max"),
                    pl.min(var).alias("min")
                ]
            )
        )
        return out
