# Import the Classification class
from Classification import Classification
from GLM import GLM


class LogisticRegression(GLM, Classification):
    def __init__(self):
        super().__init__()
        # Initialize the class
        pass

    def fit(self):
        # Method for fitting the model
        pass

    def predict(self):
        # Method for making predictions
        pass

    def LASSO(self):
        # Method for LASSO regularization
        pass

    def ridge(self):
        # Method for ridge regularization
        pass
