from typing import Union
import numpy as np
import pandas as pd
import polars as pl
from sklearn.metrics import confusion_matrix, f1_score, roc_auc_score, log_loss
from sklearn.calibration import calibration_curve
import matplotlib.pyplot as plt


class Classification:
    def __init__(self, x: np.ndarray, y: np.ndarray):#, y_true: Union[list, np.ndarray], y_pred: Union[list, np.ndarray]):
        """
        Initialize the Classification class.

        Parameters:
        y_true (list or numpy array): The true labels.
        y_pred (list or numpy array): The predicted labels.

        Returns:
        None
        """

        if isinstance(x, pl.DataFrame):
            x = x.to_numpy().reshape(-1, 1)

        if isinstance(y, pl.DataFrame):
            y= y.to_numpy().reshape(-1, 1)
            y= y.ravel()

        #if not isinstance(y_true, (list, np.ndarray)) or not isinstance(y_pred, (list, np.ndarray)):
            #raise TypeError("y_true and y_pred must be a list or numpy array.")

        #if len(y_true) != len(y_pred):
            #raise ValueError("y_true and y_pred must have the same length.")

        #self.y_true = y_true
        #self.y_pred = y_pred
        self.x = x
        self.y = y
        self.coefficients = None

    def predict(self, X: np.ndarray) -> np.ndarray:
        """
        Predict binary classification output based on a numpy array of independent variables and a vector of coefficients.

        Parameters:
        X (numpy array): Numpy array of independent variables.
        coef (numpy array): Vector of coefficients between 0 and 1.

        Returns:
        numpy array: Binary classification output.
        """
        if isinstance(X, pl.DataFrame):
            X = X.to_numpy()

        if not isinstance(X, np.ndarray) or not isinstance(self.coefficients, np.ndarray):
            raise TypeError("X and coef must be numpy arrays.")

        if X.shape[1] != self.coefficients.shape[0]:
            raise ValueError("X and coef must have compatible dimensions.")

        y_pred = X.dot(self.coefficients)

        return y_pred

    def conf_matrix(self) -> np.ndarray:
        """
        Create a confusion matrix.

        Parameters:
        None

        Returns:
        numpy array: The confusion matrix.
        """
        return confusion_matrix(self.y_true, self.y_pred)

    def f_score(self, beta: float = 1.0) -> float:
        """
        Calculate the F-score.

        Parameters:
        beta (float): The weight of precision in the calculation (default=1.0).

        Returns:
        float: The F-score.
        """
        return f1_score(self.y_true, self.y_pred, beta=beta)

    def auc_roc(self) -> float:
        """
        Calculate the AUC-ROC curve.

        Parameters:
        None

        Returns:
        float: The AUC-ROC score.
        """
        return roc_auc_score(self.y_true, self.y_pred)

    def log_loss(self) -> float:
        """
        Calculate the log loss.

        Parameters:
        None

        Returns:
        float: The log loss.
        """
        return log_loss(self.y_true, self.y_pred)

    def calibration(self) -> None:
        """
        Create a calibration plot.

        Parameters:
        None

        Returns:
        None
        """
        fraction_of_positives, mean_predicted_value = calibration_curve(self.y_true, self.y_pred, n_bins=10)
        plt.plot(mean_predicted_value, fraction_of_positives, 's-', label='Model')
        plt.plot([0, 1], [0, 1], '--', color='gray', label='Perfectly calibrated')
        plt.title('Calibration plot')
        plt.xlabel('Mean predicted value')
        plt.ylabel('Fraction of positives')
        plt.legend()
        plt.show()

    def SomersD(self) -> float:
        """
        Calculate Somers' D, which is a measure of rank correlation between two variables.

        Parameters:
        None

        Returns:
        float: Somers' D score.
        """
        y_true, y_pred = pd.Series(self.y_true), pd.Series(self.y_pred)
        n1 = (y_true == 1).sum()
        n0 = (y_true == 0).sum()
        D = ((y_pred.rank() - y_true.rank()) * (y_true - y_pred)).sum() / (n1 * n0)
        return D
