import pandas as pd
import requests
import sqlite3
import io
import os

def fetch_data(url, table_name, database_path):
    # This function tries to fetch the data, load it to sql and return its dataframe for further processing
    
    # Fetch CSV file from url
    response = requests.get(url)

    # Check if it can fetch the data successfully
    if response.status_code == 200:
        
        # Create a pandas DataFrame of the data
        df = pd.read_csv(io.StringIO(response.text))

        # Connect to to SQLite3 database
        conn = sqlite3.connect(database_path)
        cursor = conn.cursor()

        # Load the data into SQL
        df.to_sql(table_name, conn, if_exists='replace', index=False)

        conn.close()

        return df

    else:
          print(f"Cannot fetch the data.")


def get_database_path():
    # This function tries to first make a data directory for the project and return the database path in that directory
    
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


def load_sql(df, table, database_path):
    #This function tries to load the preprocessed data into the SQL database
    
    conn = sqlite3.connect(database_path)
    df.to_sql(table, conn, if_exists='replace', index=False)
    conn.close()
    

class Preprocessor():
    # In this class, all of the methods for running a well preprocessing are implemented.
    
    def __init__(self):
        # These columns should be dropped in drop_columns function.
        self.unused_columns = ['ï»¿ObjectId', 'ISO2', 'ISO3', 'Indicator', 'Unit', 'Source',
       'CTS_Code', 'CTS_Name', 'CTS_Full_Descriptor']

    def count_missing_values(self, df, axis):
        # This function, count the number of Nan values, which can be used in further preprocessing to drop invalid values.
        # If axis = 0, it counts the number of Nan values in each column
        # If axis = 1, it counts the number of Nan values in each row
        
        missing_values_count = pd.DataFrame(df.isna().sum(axis=axis)).reset_index()
        missing_values_count.columns = ['column/row', 'Nan_count']
        return missing_values_count

    def drop_columns(self, df, columns_drop_thresh):
        # This function, aims to remove invalid or non-functional columns.
        
        #First, it removes all unnecessary functions.
        df = df.drop(columns=self.unused_columns)
        
        # Second, it checks if the number of missing values in each column exceedes the threshold or not.
        # Then, tries to delete the columns with high number of missing values, by giving the column names to drop function.
        missing_values_count = self.count_missing_values(df, axis=0)
        columns_to_drop = list(missing_values_count[missing_values_count.Nan_count > columns_drop_thresh]['column/row'])
        df = df.drop(columns=columns_to_drop)

        return df
    
    def drop_rows(self, df, rows_drop_thresh):
        # This function, first check if the number of missing values in each row exceedes the threshold or not.
        # Then, tries to delete the rows with high number of missing values, by giving the list of row indices to drop function.
        
        missing_values_count = self.count_missing_values(df, axis=1)
        rows_to_drop = list(missing_values_count[missing_values_count.Nan_count > rows_drop_thresh]['column/row'])
        df = df.drop(rows_to_drop)
        return df


    def transform(self, df, columns_drop_thresh, rows_drop_thresh):
        # This function, runs all necessary preprocessing methods to the data.
        
        df = self.drop_columns(df, columns_drop_thresh)
        df = self.drop_rows(df, rows_drop_thresh)

        return df



def main():
    database_path = get_database_path()

    # Define data sources and names
    url_temperatur = "https://opendata.arcgis.com/datasets/4063314923d74187be9596f10d034914_0.csv"
    table_temperature_name = "Annual_Surface_Temperature"
    
    url_forests = "https://opendata.arcgis.com/datasets/66dad9817da847b385d3b2323ce1be57_0.csv"
    table_forests_name = "Forest_and_Carbon"

    # Fetch the data and save them in pandas dataframes
    df_temperature = fetch_data(url_temperatur, table_temperature_name, database_path)
    df_forests = fetch_data(url_forests, table_forests_name, database_path)
    
    # Preprocess the data
    preprocessor = Preprocessor()
    df_temperature = preprocessor.transform(df_temperature, df_temperature.shape[0]//2, df_temperature.shape[1]//2)
    df_forests = preprocessor.transform(df_forests,  df_forests.shape[0]//2, df_forests.shape[1]//2)
    print(df_temperature)
    print(df_forests)
    
    # Load dataframes into SQL database
    load_sql(df_temperature, table_temperature_name, database_path)
    load_sql(df_forests, table_forests_name, database_path)

if __name__ == "__main__":
    main()

    
