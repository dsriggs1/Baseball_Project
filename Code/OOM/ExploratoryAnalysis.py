from typing import Callable

import polars as pl

from Code.Production.create_model_ready import Database


class ExploratoryAnalysis(Database):
    def __init__(self, df, uid, pwd, host, db, port):
        super().__init__(uid, pwd, host, db, port)
        self.df = df


class Plotting(ExploratoryAnalysis):
    def __init__(self, df, uid, pwd, host, db, port):
        super().__init__(df, uid, pwd, host, db, port)

    def scatterplot(self):
        # Method for plotting scatterplots
        pass

    def histogram(self):
        # Method for plotting histograms
        pass

    def heatmap(self):
        # Method for plotting heatmaps
        pass

    def boxplot(self):
        # Method for plotting boxplots
        pass

    def barplot(self):
        # Method for plotting barplots
        pass

    def timeseries(self):
        # Method for plotting timeseries plots
        pass

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
            list(map(lambda var: print(df[var].dtype), var_list))

        def change_var_type(self):
            # Method for converting variable types
            pass

        def standardize(self):
            """Standardize a given list of columns in a dataframe.

            Parameters:
                df (polar.DataFrame): The dataframe to be standardized.
                var_list (list): A list of column names in the dataframe to be standardized.

            Returns:
                polar.DataFrame: The input dataframe with the specified columns standardized. The column names are suffixed with "_standard".
            """
            out = df.with_columns([
                ((pl.col(var_list) + pl.col(var_list).mean())
                 / pl.col(var_list).std()).suffix("_standard")
            ])
            return out

        def min_max_scaling(self):
            # Method for scaling variables
            pass

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
            out = df.with_columns(numpy_func(pl.col(var_list)).suffix(var_suffix))
            return out

        def create_interaction(self):
            # Method for creating interaction variables
            pass

        def create_yeo_johnson(self):
            # Method for creating yeo-johnson variables
            pass

    def corr(self):
        # Method for calculating correlations
        pass

    def missing_val(self):
        # Method for handling missing values
        pass

    def vif(self):
        # Method for calculating variance inflation factor
        pass

    def categorical_summary(self, df, var, groupby_var):
        # method for summarizing categorical variables
        q = (
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
        return q.collect()

    def binning(self):
        # Method for binning variables
        pass

    def one_hot_encoding(self):
        # Method for one-hot encoding variables
        pass

    def label_encoding(self):
        # Method for label encoding variables
        pass