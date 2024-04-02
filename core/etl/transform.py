from pandas import DataFrame
import re
import pandas as pd
import numpy as np
from typing import List, Union

from core.utils.str import remover_acentos
from core.exceptions.xl import ColunaDadosNaoEncontrada

from .parsers import ParseProc
from .sof_data_table import SofDataTable

from config import COL_PROCESSO, SAMPLE_SIZE

COL_PROCESSO_LIMPO = 'processo_limpo'


class Transformer:

    def __init__(self):
        
        self.__parse_proc = ParseProc()
        self.__build_sof_table = SofDataTable()

    def __clean_col(self, col:str)->str:

        col = col.lower().rstrip().lstrip()
        col = remover_acentos(col)

        return col

    def __find_col(self, pattern:str, df:DataFrame)->str:
        '''Returns first match'''

        for col in df.columns:
            col_clean = self.__clean_col(col)
            if re.search(pattern, col_clean):
                return col
        else:
            raise ColunaDadosNaoEncontrada(f'Padrao: {pattern}. Colunas: {df.columns}')
        

    def __add_index(self, df:DataFrame)->DataFrame:

        df = df.copy()

        df['index'] = df.index

        return df

    def __filter_df(self, df:DataFrame)->DataFrame:

        col = self.__find_col('idrgp', df)

        filtro = df[col].str.lower().str.contains('sim')

        return df[filtro].copy().reset_index(drop=True)
    
    def __parsed_proc_col(self, df:DataFrame)->DataFrame:

        df = df.copy()
        col_proc = self.__find_col(COL_PROCESSO, df)
        df[COL_PROCESSO_LIMPO] = df[col_proc].apply(self.__parse_proc)

        return df

    def __filter_and_explode(self, df_proc_clean:DataFrame)->DataFrame:

        df = df_proc_clean.copy()
        cols_proc = ['index', COL_PROCESSO_LIMPO]

        df = df[cols_proc]

        return df.explode(COL_PROCESSO_LIMPO)
    
    def __get_sof_data(self, df_exploded:DataFrame)->DataFrame:

        procs_unicos = df_exploded[COL_PROCESSO_LIMPO].unique()

        df_sof = self.__build_sof_table(procs_unicos)

        return df_sof


    def __media_proc(self, df_merged:DataFrame)->DataFrame:

        grouped = df_merged[[COL_PROCESSO_LIMPO, 'empenho', 'liquidado']].groupby(COL_PROCESSO_LIMPO)
        # se todos forem iguais, a media sera o valor original
        valor_original = grouped.mean() 
        # agora pegamos quantas vezes aparece
        counts = grouped.count()

        #e dividimos para ter a media
        medias = valor_original/counts

        medias = medias.reset_index()
        medias.rename({'empenho' : 'empenho_deduplicado', 'liquidado' : 'liquidado_deduplicado'}, axis=1, inplace=True)

        return medias

    def __add_to_pivot(self, df_exploded:DataFrame, other:DataFrame)->DataFrame:

        return pd.merge(df_exploded, other, on=COL_PROCESSO_LIMPO, how='left')
    
    def __col_to_string(self, df:DataFrame, col:str)->pd.Series:

        df = df.copy()
        return df[col].fillna('Não encontrado').astype(str)
    

    def __concat_str_cols(self, df_exploded:DataFrame)->DataFrame:

        df = df_exploded.copy()
        #basically all the cols
        cols_str = [COL_PROCESSO_LIMPO, 'dotacao', 'empenho', 'liquidado', 'status',
                    'empenho_deduplicado', 'liquidado_deduplicado']
        
        for col in cols_str:
            df[col] = self.__col_to_string(df, col)
        
        cols_agrupar = ['index'] + cols_str

        grouped = df[cols_agrupar].groupby('index').agg('; '.join).reset_index()

        grouped.rename({'empenho_deduplicado' : 'log_empenho_deduplicado',
                        'liquidado_deduplicado' : 'log_liquidado_deduplicado'},
                        axis=1, inplace=True)

        return grouped
    
    def __sum_float_cols(self, df_exploded:DataFrame)->DataFrame:

        df = df_exploded.copy()

        cols_float = ['empenho_deduplicado', 'liquidado_deduplicado']

        for col in cols_float:
            df[col].fillna(0, inplace=True)

        cols_agrupar = ['index'] + cols_float
        grouped = df[cols_agrupar].groupby('index').sum().reset_index()

        return grouped

    def __sample(self, df:DataFrame)->DataFrame:

        return df.sample(SAMPLE_SIZE)

    def __pipeline(self, df:DataFrame, sample:bool)->DataFrame:

        df = self.__add_index(df)
        df = self.__filter_df(df)

        if sample:
            df = self.__sample(df)

        df = self.__parsed_proc_col(df)
        
        df_exploded = self.__filter_and_explode(df)
        #jogar fora col processo parseado
        df.drop('processo_limpo', axis=1, inplace=True)

        #dados do sof
        df_sof = self.__get_sof_data(df_exploded)
        df_exploded = self.__add_to_pivot(df_exploded, df_sof)

        medias = self.__media_proc(df_exploded)
        df_exploded = self.__add_to_pivot(df_exploded, medias)

        df_imploded_str = self.__concat_str_cols(df_exploded)
        df_imploded_sum = self.__sum_float_cols(df_exploded)

        df_imploded = pd.merge(df_imploded_sum, df_imploded_str, on='index', how='outer')

        assert df_imploded['index'].duplicated().any() == False

        final_merge = pd.merge(df, df_imploded, how='left', on='index')

        return final_merge

    def __call__(self, df:DataFrame, sample=False)->DataFrame:

        return self.__pipeline(df, sample)
