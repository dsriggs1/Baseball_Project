from Classification import Classification
from Regression import Regression
class Tree(Classification, Regression):
    def __init__(self):
        super().__init__()
        self.model = None

    def get_depth(self):
        pass

    def get_n_leaves(self):
        pass
    def train(self, X_train, y_train):
        pass

    def predict(self, X):
        pass

    def cross_validate(self, X, y, k):
        pass

