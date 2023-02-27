# Import the Classification class
from sklearn.linear_model import LogisticRegression as lr
import polars as pl

class LogisticRegression():
    """Custom implementation of logistic regression using scikit-learn's
    LogisticRegression class designed to be able to handle polars dataframes as input
    """

    def __init__(self, C=1.0, penalty='l2', solver='lbfgs', max_iter=100):
        """Initialize logistic regression model with hyperparameters"""
        self.model = LR(C=C, penalty=penalty, solver=solver, max_iter=max_iter)
        self.coef_ = None

    def fit(self, x, y):
        """Fit logistic regression model to the input data"""
        if isinstance(x, pl.DataFrame):
            x = x.to_numpy().reshape(-1, 1)
        if isinstance(y, pl.DataFrame):
            y = y.to_numpy().reshape(-1, 1)
            y= y.ravel()

        self.model.fit(x, y)
        self.coef_ = self.model.coef_

        return self.model

    def predict(self, x):
        """Predict output labels for the input data"""
        if isinstance(x, pl.DataFrame):
            x = x.to_numpy().reshape(-1, 1)

        return self.model.predict(x)

    def LASSO(self):
        # Method for LASSO regularization
        pass

    def ridge(self):
        # Method for ridge regularization
        pass
