from typing import Callable

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

    def scatterplot(self, x, y, i=None, axs=None, marker=None, xlabel=None, ylabel=None, title=None, color=None,
                    edgecolor=None,
                    alpha=None, label="Legend", loc="best"):
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

    def histogram(self, x, y, i=None, axs=None, xlabel=None, ylabel=None, bins=None, title=None, color=None,
                  label="Legend", loc="best"):
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
            return out.collect()

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

        def missing_val(self):
            # Method for handling missing values
            pass

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
    def pearson_corr(self, var_list):
        # Method for calculating pearson correlations
        corr_df = self.df.select([
            pl.col(var_list)
        ]).collect()
        return corr_df.pearson_corr()

    def spearman_corr(self, var_list):
        # Method for calculating spearman correlations

        pass

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
