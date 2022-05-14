import unittest
import Write as wr
import yaml
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import os


class MyTestCase(unittest.TestCase):

    def test_atomicwrite_if_file_not_existed_before(self):
        # remove existed file
        if os.path.exists("test.parquet"):
            os.remove("test.parquet")

        # write table
        df = pd.DataFrame({'one': [-1, np.nan, 2.5],
                           'two': ['foo', 'bar', 'baz'],
                           'three': [False, True, False]},
                          index=list('abc'))
        table = pa.Table.from_pandas(df)
        wr.write("test.parquet", table)

        # read table for checking
        df = pq.read_table('test.parquet', columns=['three']).to_pandas()
        # check the column 'three'
        self.assertEqual(df.loc[0, "three"], False)
        self.assertEqual(df.loc[1, "three"], True)
        self.assertEqual(df.loc[2, "three"], False)

    def test_atomicwrite_if_file_existed_before(self):
        # write table
        df = pd.DataFrame({'one': [-1, np.nan, 2.5],
                           'two': ['foo', 'bar', 'baz'],
                           'three': [True, False, True]},
                          index=list('abc'))
        table = pa.Table.from_pandas(df)
        pq.write_table(table, 'test.parquet')

        # rewrite table
        df = pd.DataFrame({'one': [-1, np.nan, 2.5],
                           'two': ['foo', 'bar', 'baz'],
                           'three': [False, True, False]},
                          index=list('abc'))
        table = pa.Table.from_pandas(df)
        wr.write("test.parquet", table)

        # read table for checking
        df = pq.read_table('test.parquet', columns=['three']).to_pandas()
        # check the column 'three'
        self.assertEqual(df.loc[0, "three"], False)
        self.assertEqual(df.loc[1, "three"], True)
        self.assertEqual(df.loc[2, "three"], False)


if __name__ == '__main__':
    unittest.main()
