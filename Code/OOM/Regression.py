import numpy as np
import polars as pl
class Regression:
    """
    Base class for regression based models. Not intended to be directly called.
    Intended for subclassing.
    """
    def __init__(self, x: np.ndarray, y: np.ndarray):
        if isinstance(x, pl.DataFrame):
            x = x.to_numpy()

        if isinstance(y, pl.DataFrame):
            y = y.to_numpy()

        if not isinstance(x, np.ndarray):
            raise TypeError("Input data X must be a numpy.ndarray")

        self.x = x
        self.y = y
        self.coefficients = None

    def predict(self, X: np.ndarray) -> np.ndarray:
        """Make predictions using the fitted regression model.

        Parameters:
        X (numpy.ndarray): Input data of shape (n_samples, n_features).

        Returns:
        numpy.ndarray: Predicted target variable of shape (n_samples,).
        """
        if isinstance(X, pl.DataFrame):
            X = X.to_numpy()

        if not isinstance(X, np.ndarray):
            raise TypeError("Input data X must be a numpy.ndarray")

        if not np.all(X[:, 0] == 1):  # Check if bias term exists
            X = np.insert(X, 0, 1, axis=1)  # Add bias term

        return X.dot(self.coefficients)

    def RMSE(self) -> float:
        """Calculate the root mean squared error (RMSE) for the regression model.

        Returns:
        float: Root mean squared error (RMSE) of the predictions.
        """

        y_pred = self.predict(self.x)
        return np.sqrt(np.mean((self.y - y_pred) ** 2))

    def MSE(self) -> float:
        """Calculate the mean squared error (MSE) for the regression model.

        Returns:
        float: Mean squared error (MSE) of the predictions.
        """

        y_pred = self.predict(self.x)
        return np.mean((self.y - y_pred) ** 2)

    def MAE(self) -> float:
        """Calculate the mean absolute error (MAE) for the regression model.

        Returns:
        float: Mean absolute error (MAE) of the predictions.
        """

        y_pred = self.predict(self.x)
        return np.mean(np.abs(self.y - y_pred))

