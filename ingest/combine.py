import datetime, os
import pandas as pd

_FILENAME_COMBINED = 'data/daily/us.combined.csv'

def _combine_dfs(dfs):
    df_combined = pd.DataFrame()
    for df in dfs:
        df_combined = df_combined.append(df)

    df_combined = df_combined.loc[~df_combined.index.duplicated(keep='first')].sort_index()
    return df_combined

def _combine_files(filenames, index_col):
    dfs = [pd.read_csv(fn, index_col=index_col) for fn in filenames]
    return _combine_dfs(dfs)

def combine_and_save_files(dir, index_col):
    filenames = [os.path.join(dir, fn) for fn in os.listdir(dir) if '.csv' in fn]

    df_combined = _combine_files(filenames, index_col)
    df_combined.to_csv(_FILENAME_COMBINED)
    return df_combined



