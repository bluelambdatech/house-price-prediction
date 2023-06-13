import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier as RFC
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import RandomForestRegressor as RFR
from sklearn.metrics import mean_squared_error as mse
import numpy as np

class Train_Predict ():
    
    def Test_Train(self, df, test_size):
        "this method is used to select the columns, split the dataset into test and train data"
        X, y = df.iloc[:, 3:].values, df.iloc[:, 2].values
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3, random_state=0)

        return df[final_columns], X_train, X_test, y_train, y_test

    def Feature_Selection (self, n_estimators, random_state):
        rfc = RFC(n_estimators = 5, random_state=1)
        rfc.fit(X_train, y_train)
        importances = rfc.feature_importances_
        indices = np.argsort(importances)[::-1]

        feat_labels = df.columns[3:]
        for f in range(X_train.shape[1]):
            print("%2d) %-*s %f" % (f + 1, 30, feat_labels[indices[f]], importances[indices[f]]))
        ff = df.iloc[:, 3:]
        cols = []
        for i in indices:
            cols.append(ff.columns[i])
        X, y = df[final_cols].values, df[['price']].values
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3, random_state=0)

        return df[final_columns], X_train, X_test, y_train, y_test

    def Predict(self,x_test):
        rfr = RFR()
        rfr.fit(X_train, y_train)
        rfr.predict(X_test)

        return mse(y_test, rfr.predict(X_test))






