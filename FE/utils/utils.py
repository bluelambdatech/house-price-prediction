import pandas as pd


class FeatureEngineering:

    def __init__(self, df):
        self.df = df

    def extractDayMonthYear(self):
        """

        """
        self.df['date'] = pd.to_datetime(self.df['date'])
        self.df.insert(2, 'day', self.df['date'].dt.day)
        self.df.insert(3, 'month', self.df['date'].dt.month)
        self.df.insert(4, 'year', self.df['date'].dt.year)
        print(self.df.head())

    def run_process(self):
        self.extractDayMonthYear()





if __name__ == "__main__":

    transform = FeatureEngineering(df)
    transform.run_process()