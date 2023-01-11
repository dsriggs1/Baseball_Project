import polars as pl


class ExploratoryAnalysis:
    def __init__(self):
        # Initialize the class
        pass

    def plot(self):
        # Method for plotting
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

    def check_var_type(self):
        # Method for checking variable type
        pass

    def change_var_type(self):
        # Method for converting variable types
        pass

    def standardize(self):
        # Method for standardizing variables
        pass

    def min_max_scaling(self):
        # Method for scaling variables
        pass

    def transform_vars(self, df: pl.DataFrame, var_list: List[str],
                       numpy_func: Callable, var_suffix: str) -> pl.DataFrame:
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
