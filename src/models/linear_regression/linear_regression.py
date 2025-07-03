from sklearn.linear_model import LinearRegression
from base_model import BaseModel
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

class LinearRegressionModel(BaseModel):
    def __init__(self):
        self.pipeline = Pipeline([
            ('scaler', StandardScaler()),
            ('regressor', LinearRegression())
        ])

    def train(self, X_train, y_train):
        self.pipeline.fit(X_train, y_train)

    def predict(self, X_test):
        return self.pipeline.predict(X_test)

    def evaluate(self, X_test, y_test):
        from sklearn.metrics import mean_squared_error, r2_score
        predictions = self.predict(X_test)
        mse = mean_squared_error(y_test, predictions)
        r2 = r2_score(y_test, predictions)
        return {"mse": mse, "r2": r2}
