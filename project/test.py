import sqlite3
import unittest
import numpy as np
import pandas as pd
from io import StringIO
from pipeline import *
from pandas.testing import assert_frame_equal


# With Test Pipeline class using unittest we are able to test each function in data pipeline
class TestPipeline(unittest.TestCase):
    
    def test_csv_interpreter(self):
        # Create a mock csv data using StringIO and convert it to data frame using csv_interpreter
        data = StringIO("column0, column1, column2\n0,1,2\n3,4,5")
        df = csv_interpreter(data)
        
        # Check if it returns a pandas data frame
        assert isinstance(df, pd.DataFrame)
        
        # Check the size of the data frame
        assert df.shape == (2, 3)

    def test_transform(self):
        # Create a mock data frame and apply transformations on it using the Preprocessor class
        df = pd.DataFrame([[0, 1, 2, 3], [4, 5, 6, 7], [8, 9, np.nan, np.nan], [12, 13, np.nan, 15]], columns=['column0', 'column1', 'column2', 'column3'])
        unused_columns = ['column0']
        
        preprocessor = Preprocessor(df, unused_columns)
        df = preprocessor.transform()
        
        # Check the size of the transformed data frame
        assert df.shape == (4, 2)
        
    
    def test_load(self):
        # Create a mock data frame and aload it SQLite database using load function
        df = pd.DataFrame([[0, 1, 2], [3, 4, 5]], columns=['column0', 'column1', 'column2'])
        load_sql(df, "test_table", 'test.sqlite')
        
        conn = sqlite3.connect('test.sqlite')
        result = pd.read_sql_query("SELECT * FROM test_table", conn)
        conn.close()
        
        assert_frame_equal(result, df)
        
        
if __name__ == '__main__':
    unittest.main()
