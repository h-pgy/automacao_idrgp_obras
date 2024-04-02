import pandas as pd
import numpy as np

from typing import List
from core.api import ApiSof
from .parsers import ExtractApiVal

class SofDataTable:

    def __init__(self)->None:

        self.__parse_api_resp = ExtractApiVal()
        self.__sof = ApiSof()

    def __consult_sof(self, proc:int)->dict:

        api_resp = self.__sof.get_empenho_last_year_by_proc(proc)

        return api_resp

    def __build_proc_data(self, proc:int)->dict:

        try:
            api_resp = self.__consult_sof(proc)
            proc_data = {
                'processo_limpo' : proc,
                'dotacao' : self.__parse_api_resp(api_resp, 'dotacao'),
                'empenho' : self.__parse_api_resp(api_resp, 'empenho'),
                'liquidado' : self.__parse_api_resp(api_resp, 'liquidacao'),
                'status' : 'OK'
            }

        except Exception as e:
            proc_data = {
                'processo_limpo' : proc,
                'dotacao' : '',
                'empenho' : np.NaN,
                'liquidado' : np.NaN,
                'status' : f'{type(e).__name__}: {str(e)}'
            }

        return proc_data
    
    def __build_table(self, procs_unicos)->pd.DataFrame:

        parsed = [self.__build_proc_data(proc) for proc in procs_unicos]

        return pd.DataFrame(parsed)
    
    def __call__(self, procs_unicos=List[int])->pd.DataFrame:

        return self.__build_table(procs_unicos)