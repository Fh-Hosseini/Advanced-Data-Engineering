import sqlite3
import unittest
import numpy as np
import pandas as pd
from io import StringIO
from project.pipeline import *
from pandas.testing import assert_frame_equal


# With Test Pipeline class using unittest we are able to test each function in data pipeline
class TestPipeline(unittest.TestCase):
    
    def test_csv_interpreter(self):
        # Create a mock csv data using StringIO and convert it to data frame using csv_interpreter
        data = StringIO("column0, column1, column2\n0,1,2\n3,4,5")
        df = csv_interpreter(data)
        
        # Check if it returns a pandas data frame
        assert isinstance(df, pd.DataFrame), "CSV Interpreter does not return a pandas data frame"
        
        # Check the size of the data frame
        assert df.shape == (2, 3), "The shape of the data frame is not correct"

    def test_transform(self):
        # Create a mock data frame and apply transformations on it using the Preprocessor class
        df = pd.DataFrame([[0, 1, 2, 3], [4, 5, 6, 7], [8, 9, np.nan, np.nan], [12, 13, np.nan, 15]], columns=['column0', 'column1', 'column2', 'column3'])
        unused_columns = ['column0']
        
        preprocessor = Preprocessor(df, unused_columns)
        df = preprocessor.transform()
        
        # Check the size of the transformed data frame
        assert df.shape == (4, 2), "The transformation is not successful"
        
    
    def test_load(self):
        # Create a mock data frame and aload it SQLite database using load function
        df = pd.DataFrame([[0, 1, 2], [3, 4, 5]], columns=['column0', 'column1', 'column2'])
        load_sql(df, "test_table", 'test.sqlite')
        
        conn = sqlite3.connect('test.sqlite')
        result = pd.read_sql_query("SELECT * FROM test_table", conn)
        conn.close()
        
        assert_frame_equal(result, df), "There is a problem in loading the data into sqlite database"
        
    
    def test_etl_pipeline(self):
        url = "https://opendata.arcgis.com/datasets/4063314923d74187be9596f10d034914_0.csv"
        unused_columns = ['ObjectId', 'ISO2', 'ISO3', 'Indicator', 'Unit', 'Source', 'CTS_Code', 'CTS_Name', 'CTS_Full_Descriptor']
        df = etl_pipeline(url, "test_temperature", unused_columns)

        # Check if it returns a pandas data frame
        assert isinstance(df, pd.DataFrame)
        
        # Check if the dataframe saves correctly and it is not empty
        assert not df.empty
    
    
    def test_analyse_pipeline(self):
        url = "https://opendata.arcgis.com/datasets/4063314923d74187be9596f10d034914_0.csv"
        df_temperature = etl_pipeline(url, "test_temperature", [])
        
        url2 = "https://opendata.arcgis.com/datasets/66dad9817da847b385d3b2323ce1be57_0.csv"
        df_forest = etl_pipeline(url, "test_forest", [])

        dfs_climate = analyse_pipeline(df_temperature, df_forest)
        
        for table_name, df in dfs_climate.items():
            # Check if it returns a pandas data frame
            assert isinstance(df, pd.DataFrame)
        
            # Check if the dataframe saves correctly and it is not empty
            assert not df.empty
        
        
if __name__ == '__main__':
    unittest.main()
