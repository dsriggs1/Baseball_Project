from Code.OOM.Classification import Classification
from sklearn.linear_model import LogisticRegression as LR
import polars as pl
import numpy as np
class LogisticRegression(Classification):
    """Custom implementation of logistic regression using scikit-learn's
    LogisticRegression class designed to be able to handle polars dataframes as input
    """

    def __init__(self, x: np.ndarray, y: np.ndarray, C=1.0, penalty='l2', solver='lbfgs', max_iter=100):
        super().__init__(x, y)
        """Initialize logistic regression model with hyperparameters"""
        self.model = LR(C=C, penalty=penalty, solver=solver, max_iter=max_iter)
        self.coefficients = None

    def fit(self):
        """Fit logistic regression model to the input data"""

        self.model.fit(self.x, self.y)
        self.coefficients = self.model.coef_

        return self.model
    def LASSO(self):
        # Method for LASSO regularization
        pass

    def ridge(self):
        # Method for ridge regularization
        pass
