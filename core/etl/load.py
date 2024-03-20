from pandas import DataFrame

from core.utils.io import solve_path
from config import GENERATED_DATA_DIR


class Load:

    def __init__(self, fname:str)->None:

        self.fpath = solve_path(fname, GENERATED_DATA_DIR)
    
    def __call__(self, df:DataFrame, return_df=False)->str:

        df.to_excel(self.fpath)

        if return_df:
            return df

        return self.fpath