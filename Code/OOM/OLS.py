# Import the Regression class
from Regression import Regression
import pandas as pd
import numpy as np
import polars as pl
import matplotlib as plt
from scipy.stats import shapiro
from scipy.stats import bartlett, levene
from statsmodels.stats.diagnostic import het_breuschpagan, normal_ad
from statsmodels.graphics.tsaplots import plot_acf
from statsmodels.stats.diagnostic import acorr_ljungbox
from statsmodels.stats.stattools import durbin_watson
from statsmodels.stats.api import linear_rainbow
from statsmodels.stats.outliers_influence import variance_inflation_factor

class OLS(Regression):
    def __init__(self):
        super().__init__()
        # Initialize the class
        pass

    def fit(self, x: np.ndarray, y: np.ndarray) -> None:
        """Fit the regression model to the input data.

        Parameters:
        X (numpy.ndarray): Input data of shape (n_samples, n_features).
        y (numpy.ndarray): Target variable of shape (n_samples,).

        Returns:
        None
        """
        if isinstance(x, pl.DataFrame):
            x = x.to_numpy()
        if isinstance(y, pl.DataFrame):
            y = y.to_numpy()

        if not isinstance(x, np.ndarray):
            raise TypeError("Input data X must be a numpy.ndarray")
        if not isinstance(y, np.ndarray):
            raise TypeError("Target variable y must be a numpy.ndarray")
        x = np.insert(x, 0, 1, axis=1)  # Add bias term
        self.coefficients = np.linalg.inv(x.T.dot(x)).dot(x.T).dot(y)

    def normality(self) -> None:
        """Plot a histogram of the residuals to test for normality."""
        residuals = self.predict(self.X) - self.y
        plt.hist(residuals, bins=20, density=True)
        plt.title('Residuals distribution')
        plt.xlabel('Residuals')
        plt.ylabel('Density')
        plt.show()

        # Perform Shapiro-Wilk test for normality
        alpha = 0.05
        stat, p = shapiro(residuals)
        if p > alpha:
            print('Residuals are normally distributed (fail to reject H0)')
        else:
            print('Residuals are not normally distributed (reject H0)')

    def heteroscedasticity(self, alpha: float = 0.05) -> None:
        """Plot residuals against predicted values to test for heteroscedasticity.

        Parameters:
            alpha (float): The significance level of the test (default=0.05).

        Returns:
            None
        """
        residuals = self.predict(self.X) - self.y
        plt.scatter(self.predict(self.X), residuals)
        plt.title('Residuals vs. Predicted values')
        plt.xlabel('Predicted values')
        plt.ylabel('Residuals')
        plt.axhline(y=0, color='r', linestyle='-')
        plt.show()

        # Perform Bartlett, Levene or Breusch-Pagan test for heteroscedasticity
        try:
            stat, p = bartlett(self.y, residuals)
            test_name = "Bartlett's test"
        except:
            try:
                stat, p = levene(self.y, residuals)
                test_name = "Levene's test"
            except:
                lm, lm_pvalue, fvalue, f_pvalue = het_breuschpagan(residuals, self.X)
                stat = fvalue
                p = f_pvalue
                test_name = "Breusch-Pagan test"

                # Check normality and independence of errors
                jb_pvalue = normal_ad(residuals)[1]
                if jb_pvalue < alpha:
                    print(
                        'Warning: Residuals are not normally distributed, the Breusch-Pagan test result may be invalid.')
                if np.abs(np.corrcoef(residuals[1:], residuals[:-1])[0][1]) > alpha:
                    print('Warning: Residuals are not independent, the Breusch-Pagan test result may be invalid.')

        if p > alpha:
            print('Residuals are homoscedastic (fail to reject H0)')
        else:
            print(f'Residuals are not homoscedastic (reject H0, using {test_name})')

    def multicollinearity(self, vif_thresh=5.0) -> None:
        """Test for multicollinearity using VIF values."""
        X = self.X.assign(intercept=1)
        vif_data = pd.DataFrame(
            {'feature': X.columns, 'vif': [variance_inflation_factor(X.values, i) for i in range(X.shape[1])]})
        vif_data = vif_data.sort_values('vif', ascending=False)
        print('VIF results:')

        high_vif_features = list(vif_data[vif_data['vif'] > vif_thresh]['feature'])
        if high_vif_features:
            print(f'The following features have VIF greater than {vif_thresh}: {high_vif_features}')
        else:
            print(f'No features have VIF greater than {vif_thresh}.')

    def autocorrelation(self, alpha: float = 0.05) -> None:
        """Plot an autocorrelation plot of the residuals and perform the Ljung-Box and Durbin-Watson tests to test for autocorrelation."""
        residuals = self.predict(self.X) - self.y
        plot_acf(residuals, lags=20)
        plt.title('Autocorrelation plot of residuals')
        plt.show()

        # Perform Ljung-Box test for autocorrelation
        lbvalue, pvalue = acorr_ljungbox(residuals, lags=20)

        if pvalue.min() > alpha:
            print('Residuals are not autocorrelated (fail to reject H0)')
        else:
            print('Residuals are autocorrelated (reject H0)')

        # Perform Durbin-Watson test for autocorrelation
        dwvalue = durbin_watson(residuals)
        if dwvalue < 1.5:
            print('Residuals are positively autocorrelated (DW < 1.5)')
        elif dwvalue > 2.5:
            print('Residuals are negatively autocorrelated (DW > 2.5)')
        else:
            print('Residuals show no evidence of autocorrelation (1.5 < DW < 2.5)')

    from statsmodels.stats.api import linear_rainbow

    def linearity(self, alpha: float = 0.05) -> None:
        """Plot predicted values against actual values to test for linearity, and perform the Rainbow test for linearity."""
        predicted = self.predict(self.X)
        residuals = predicted - self.y
        plt.scatter(self.y, predicted)
        plt.plot([self.y.min(), self.y.max()], [self.y.min(), self.y.max()], 'k--', lw=2)
        plt.title('Predicted values vs. Actual values')
        plt.xlabel('Actual values')
        plt.ylabel('Predicted values')
        plt.show()

        # Perform Rainbow test for linearity
        rainbow_stat, rainbow_p_value = linear_rainbow(self.results)

        if rainbow_p_value > alpha:
            print('Residuals are linear (fail to reject H0)')
        else:
            print('Residuals are not linear (reject H0)')

