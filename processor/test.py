import pytest
import dask.dataframe as dd
import post_input.load_input as load_input

class TestTwitterEmpty:
    def test_empty_twitter_type(self):
        df = load_input.create_empty_twitter_dataframe()
        assert type(df) == dd.core.DataFrame

    def test_empty_twitter_id(self):
        df = load_input.create_empty_twitter_dataframe()
        dtype = str(df.dtypes['id'])
        assert dtype == 'float64'

if __name__ == '__main__':
    print('usage: pytest test.py')
    exit(1)

