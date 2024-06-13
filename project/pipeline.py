import pandas as pd
import requests
import sqlite3
import io
import os
import urllib.request


# Extract data from Http link
def extract_data(url):
    return urllib.request.urlopen(url)


# Create a pandas DataFrame of the data
def csv_interpreter(data):
    return pd.read_csv(data)


# This function tries to first make a data directory for the project and return the database path in that directory
def get_database_path():
    current_directory = os.path.dirname(os.path.abspath(__file__))
    parent_directory = os.path.dirname(current_directory) 
    data_directory = os.path.join(parent_directory, 'data')

    # Check if the data directory is not exist, create it
    if not os.path.exists(data_directory):
        os.makedirs(data_directory)

    # Create the database path
    database_name = 'climate_change.db'
    database_path = os.path.join(data_directory, database_name)
    return database_path


# In this class, all of the methods for running a well preprocessing are implemented.
class Preprocessor():
    
    def __init__(self, df, unused_columns):
        self.df = df
        self.unused_columns = unused_columns
    
    
    # This function, count the number of Nan values, which can be used in further preprocessing to drop invalid values.
    # If axis = 0, it counts the number of Nan values in each column
    # If axis = 1, it counts the number of Nan values in each row
    def count_missing_values(self, axis):
        missing_values_count_df = pd.DataFrame(self.df.isna().sum(axis=axis)).reset_index()
        missing_values_count_df.columns = ['column/row', 'Nan_count']
        return missing_values_count_df
    
    
    # This function removes all unnecessary columns based on the dataframe.
    def drop_unused_columns(self):
        self.df = self.df.drop(columns = self.unused_columns).reset_index(drop=True)
        return self.df
        
        
    # This function, aims to remove nan values
    # First, it checks if the number of missing values in each column exceedes the threshold or not.
    # Then, tries to delete the columns with high number of missing values, by giving the column names to drop function.
    def dropna_columns(self, columns_drop_thresh):
        missing_values_count = self.count_missing_values(axis=0)
        columns_to_drop = list(missing_values_count[missing_values_count.Nan_count > columns_drop_thresh]['column/row'])
        self.df = self.df.drop(columns=columns_to_drop).reset_index(drop=True)
        return self.df
    
    
    # This function, first check if the number of missing values in each row exceedes the threshold or not.
    # Then, tries to delete the rows with high number of missing values, by giving the list of row indices to drop function.
    def dropna_rows(self, rows_drop_thresh):
        missing_values_count = self.count_missing_values(axis=1)
        rows_to_drop = list(missing_values_count[missing_values_count.Nan_count > rows_drop_thresh]['column/row'])
        self.df = self.df.drop(rows_to_drop).reset_index(drop=True)
        return self.df


    # This function, runs all necessary preprocessing methods to the data.
    def transform(self):
        self.drop_unused_columns()
        self.dropna_columns(self.df.shape[1] // 2)
        self.dropna_rows(self.df.shape[0] // 2)
        return self.df


# Load the data into SQL
def load_sql(data, table_name, database_path):
    conn = sqlite3.connect(database_path)
    data.to_sql(table_name, conn, if_exists='replace', index=False)
    conn.close()
    
    
def get_datasets(df_temperature, df_forest):
    
    # Keep common countries of two data frame
    countries = pd.merge(df_temperature, df_forest, on='Country', how='inner')['Country']
    df_temperature = df_temperature[df_temperature['Country'].isin(countries)]
    df_forest = df_forest[df_forest['Country'].isin(countries)]
    
    # Extract different data frames based on their Indicator
    unique_indicators = df_forest['Indicator'].unique()
    df_climate_change = {}
    for indicator in unique_indicators:
        df_climate_change[indicator] = df_forest[df_forest['Indicator'] == indicator]
    df_climate_change['Temperature'] = df_temperature
    
    return df_climate_change

    
# The ETL pipeline to Extract, Transform and Load the data into SQLite database
def etl_pipeline(data_url, table_name, unused_columns):
    
    database_path = get_database_path()
    
    # Fetch the data and save them in pandas dataframes
    data = extract_data(data_url)
    df =csv_interpreter(data)

    # Preprocess the data
    preprocessor = Preprocessor(df, unused_columns)
    df = preprocessor.transform()
    
    # Save the data into a SQLite database
    load_sql(df, table_name, database_path)
    return df
    

# Use this pipeline to extract data frames from the two main Annual_Surface_Temperature and Forest_and_Carbon data frames
# based on different indicators
def analyse_pipeline(df_temperature, df_forest):
    database_path = get_database_path()
    df_climate_change = get_datasets(df_temperature, df_forest)
    
    for table_name, df in df_climate_change.items():
        load_sql(df, table_name, database_path)
    
    return df_climate_change


def main():
    # Define data sources and names related to Annual_Surface_Temperature dataset
    temperatur_url = "https://opendata.arcgis.com/datasets/4063314923d74187be9596f10d034914_0.csv"
    table_temperature_name = "Annual_Surface_Temperature"
    unused_columns = ['ObjectId', 'ISO2', 'ISO3', 'Indicator', 'Unit', 'Source', 'CTS_Code', 'CTS_Name', 'CTS_Full_Descriptor']
    df_temperature = etl_pipeline(temperatur_url, table_temperature_name, unused_columns)
        
    # Define data sources and names related to Forest_and_Carbon dataset
    forest_url = "https://opendata.arcgis.com/datasets/66dad9817da847b385d3b2323ce1be57_0.csv"
    table_forest_name = "Forest_and_Carbon"
    unused_columns = ['ObjectId', 'ISO2', 'ISO3', 'Source', 'CTS_Code', 'CTS_Name', 'CTS_Full_Descriptor']
    df_forest = etl_pipeline(forest_url, table_forest_name, unused_columns)

    analyse_pipeline(df_temperature, df_forest)
    

if __name__ == "__main__":
    main()
