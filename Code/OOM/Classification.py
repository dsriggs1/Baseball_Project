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

        self.y_pred = X.dot(self.coefficients)
        self.y_pred = 1.0 / (1.0 + np.exp(-self.y_pred))

        return self.y_pred

    def predict_binary(self, X: np.ndarray, threshold: float = 0.5) -> np.ndarray:
        """
        Predict binary classification output based on a numpy array of independent variables,
        a vector of coefficients, and a threshold.

        Parameters:
        X (numpy array): Numpy array of independent variables.
        threshold (float): Threshold to convert probabilities to binary output. Default is 0.5.

        Returns:
        numpy array: Binary classification output (0 or 1).
        """
        if not isinstance(threshold, (float, int)):
            raise TypeError("threshold must be a float or integer.")

        if threshold < 0 or threshold > 1:
            raise ValueError("threshold must be between 0 and 1.")

        # Get the probability predictions
        y_pred_proba = self.predict(X)

        # Convert probabilities to binary classification
        self.y_pred_binary = np.where(y_pred_proba >= threshold, 1, 0)

        return self.y_pred_binary

    def conf_matrix(self) -> np.ndarray:
        """
        Create a confusion matrix.

        Parameters:
        None

        Returns:
        numpy array: The confusion matrix.
        """

        if self.y_pred_binary is None:
            raise ValueError("Unable to create confusion matrix. 'y_pred_binary' is None. Please run the 'predict_binary' method first to generate predicted values before calling the 'conf_matrix' method.")


        return confusion_matrix(self.y, self.y_pred_binary)

    def f_score(self, labels=None, pos_label=1, average='binary', sample_weight=None, zero_division='warn') -> float:
        """
        Calculate the F-score.

        Parameters:
        labels (list, optional): The set of labels to include when calculating the F-score. Default is None.
        pos_label (int or str, optional): The class to report if the average is set to 'binary'. Default is 1.
        average (str, optional): The method to compute the F-score. It can be one of {'micro', 'macro', 'weighted', 'binary', 'samples', None}. Default is 'binary'.
        sample_weight (array-like, optional): Sample weights. Default is None.
        zero_division (str or float, optional): The value to return when there is a zero division, either due to 0 true positive predictions or 0 true positive and false negative predictions. It can be one of {'warn', 0, 1}. Default is 'warn'.

        Returns:
        float: The F-score.
        """
        return f1_score(self.y, self.y_pred_binary, labels=labels, pos_label=pos_label, average=average, sample_weight=sample_weight, zero_division=zero_division)

    def auc_roc(self) -> float:
        """
        Calculate the AUC-ROC curve.

        Parameters:
        None

        Returns:
        float: The AUC-ROC score.
        """
        return roc_auc_score(self.y, self.y_pred)

    def log_loss(self) -> float:
        """
        Calculate the log loss.

        Parameters:
        None

        Returns:
        float: The log loss.
        """
        return log_loss(self.y, self.y_pred)

    def calibration(self) -> None:
        """
        Create a calibration plot.

        Parameters:
        None

        Returns:
        None
        """
        fraction_of_positives, mean_predicted_value = calibration_curve(self.y, self.y_pred, n_bins=10)
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
        y, y_pred = pd.Series(self.y), pd.Series(self.y_pred)
        n1 = (y == 1).sum()
        n0 = (y == 0).sum()
        D = ((y_pred.rank() - y.rank()) * (y - y_pred)).sum() / (n1 * n0)
        return D
