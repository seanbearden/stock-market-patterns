import unittest
import pandas as pd


class TestHDF5StoreDataFrames(unittest.TestCase):
    def setUp(self):
        sandp500_df = pd.read_csv('res/indices/s_and_p_500_details.csv')

        self.symbols = sandp500_df['Ticker'].values

    def test_data_dataframe_not_empty(self):
        # Open the HDF5 file
        with pd.HDFStore('res/data/s_and_p_data.h5', mode='r') as store:
            # Verify that each DataFrame is not empty
            for df_key in self.symbols:
                with self.subTest(df_key=df_key):  # Use subTest to check each DataFrame separately
                    df = store[df_key]
                    self.assertGreater(df.shape[0], 0, f'DataFrame {df_key} is empty.')

    def test_events_dataframes_not_empty(self):
        # Open the HDF5 file
        with pd.HDFStore('res/data/s_and_p_events.h5', mode='r') as store:
            # Verify that each DataFrame is not empty
            for df_key in self.symbols:
                with self.subTest(df_key=df_key):  # Use subTest to check each DataFrame separately
                    df = store[df_key]
                    self.assertGreater(df.shape[0], 0, f'DataFrame {df_key} is empty.')
