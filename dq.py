import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from scipy import stats


class DataQuality:
    def __init__(self, df):
        self.df = df

    def check_completeness(self):
        missing_values = self.df.isna().sum()
        zero_count = self.df[self.df['Value'] == 0].count()
        n_countries = self.df['Country'].nunique()
        total_values = self.df.count() + missing_values
        completeness = (total_values - missing_values) / total_values * 100
        df1 = pd.DataFrame({'Completeness': completeness, 'Zero_Count': zero_count})
        df2 = pd.DataFrame({'N_Countries': [n_countries] * len(df1)}, index=df1.index)
        result = pd.concat([df1, df2], axis=1)
        return result

    def treat_outliers(self, col):
        # Calculate the Z-scores
        z_scores = stats.zscore(self.df[col])
        # Calculate the absolute Z-scores
        abs_z_scores = np.abs(z_scores)
        # Identify the outliers
        outliers = abs_z_scores > 2
        # Calculate the mean and standard deviation
        mean = self.df[col].mean()
        std = self.df[col].std()
        # Cap the outliers at 3 standard deviations from the mean
        self.df.loc[outliers, col] = np.sign(z_scores[outliers]) * 2 * std + mean
        return self.df


    def show_distributions(self, name: str, year: str):
        num_cols = self.df.select_dtypes(include=['float64', 'int64']).columns
        for col in num_cols:
            plt.figure(figsize=(10, 5))
            plt.title(f"Distribution of {name} in {year}")
            df_no_outliers = self.treat_outliers(col)
            plt.hist(df_no_outliers[col].dropna(), bins=50, color='skyblue', edgecolor='black')
            plt.show()
