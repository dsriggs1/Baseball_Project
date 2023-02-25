# Import the Classification class
from sklearn.linear_model import LogisticRegression as lr
import polars as pl

class LogisticRegression():
    def __init__(self):
        super().__init__()
        self.model = lr()
        self.coef_ = None

    def fit(self, x, y):

        if isinstance(x, pl.DataFrame):
            x = x.to_numpy().reshape(-1, 1)
        if isinstance(y, pl.DataFrame):
            y = y.to_numpy().reshape(-1, 1)
            y= y.ravel()

        self.model.fit(x, y)
        self.coef_ = self.model.coef_

        return self.model


    def predict(self):
        # Method for making predictions
        pass

    def LASSO(self):
        # Method for LASSO regularization
        pass

    def ridge(self):
        # Method for ridge regularization
        pass
