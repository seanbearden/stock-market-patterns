import unittest
import pandas as pd


class TestHDF5StoreDataFrames(unittest.TestCase):
    def test_dataframes_not_empty(self):
        # Open the HDF5 file
        with pd.HDFStore('res/data/s_and_p_data.h5', mode='r') as store:
            # List all keys/paths in the store
            dataframe_keys = store.keys()

            # Verify that each DataFrame is not empty
            for df_key in dataframe_keys:
                with self.subTest(df_key=df_key):  # Use subTest to check each DataFrame separately
                    df = store[df_key]
                    self.assertGreater(df.shape[0], 0, f'DataFrame {df_key} is empty.')


# This part is for running the tests if this script is executed
if __name__ == '__main__':
    unittest.main()
