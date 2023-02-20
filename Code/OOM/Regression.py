import numpy as np

class Regression:
    def __init__(self):
        self.coefficients = None

    def predict(self, X: np.ndarray) -> np.ndarray:
        """Make predictions using the fitted regression model.

        Parameters:
        X (numpy.ndarray): Input data of shape (n_samples, n_features).

        Returns:
        numpy.ndarray: Predicted target variable of shape (n_samples,).
        """
        if not isinstance(X, np.ndarray):
            raise TypeError("Input data X must be a numpy.ndarray")
        X = np.insert(X, 0, 1, axis=1)  # Add bias term
        return X.dot(self.coefficients)

    def RMSE(self, X: np.ndarray, y: np.ndarray) -> float:
        """Calculate the root mean squared error (RMSE) for the regression model.

        Parameters:
        X (numpy.ndarray): Input data of shape (n_samples, n_features).
        y (numpy.ndarray): Target variable of shape (n_samples,).

        Returns:
        float: Root mean squared error (RMSE) of the predictions.
        """
        if not isinstance(X, np.ndarray):
            raise TypeError("Input data X must be a numpy.ndarray")
        if not isinstance(y, np.ndarray):
            raise TypeError("Target variable y must be a numpy.ndarray")
        y_pred = self.predict(X)
        return np.sqrt(np.mean((y - y_pred) ** 2))

    def MSE(self, X: np.ndarray, y: np.ndarray) -> float:
        """Calculate the mean squared error (MSE) for the regression model.

        Parameters:
        X (numpy.ndarray): Input data of shape (n_samples, n_features).
        y (numpy.ndarray): Target variable of shape (n_samples,).

        Returns:
        float: Mean squared error (MSE) of the predictions.
        """
        if not isinstance(X, np.ndarray):
            raise TypeError("Input data X must be a numpy.ndarray")
        if not isinstance(y, np.ndarray):
            raise TypeError("Target variable y must be a numpy.ndarray")
        y_pred = self.predict(X)
        return np.mean((y - y_pred) ** 2)

    def MAE(self, X: np.ndarray, y: np.ndarray) -> float:
        """Calculate the mean absolute error (MAE) for the regression model.

        Parameters:
        X (numpy.ndarray): Input data of shape (n_samples, n_features).
        y (numpy.ndarray): Target variable of shape (n_samples,).

        Returns:
        float: Mean absolute error (MAE) of the predictions.
        """
        if not isinstance(X, np.ndarray):
            raise TypeError("Input data X must be a numpy.ndarray")
        if not isinstance(y, np.ndarray):
            raise TypeError("Target variable y must be a numpy.ndarray")
        y_pred = self.predict(X)
        return np.mean(np.abs(y - y_pred))
