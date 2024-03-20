from pandas import DataFrame
import re
import pandas as pd
import numpy as np
from typing import List, Union

from core.utils.str import remover_acentos
from core.exceptions.xl import ColunaDadosNaoEncontrada
from core.exceptions.sof import ProcessoForadoPadrao

from core.api import ApiSof

from config import COL_EMPENHO, COL_LIQUIDACAO, COL_PROXY, COL_PROCESSO

COL_DOTACAO='dotacao_sof'


class Transformer:

    def __init__(self):
        
        self.sof = ApiSof()

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

    def __assert_processo_clean(self, proc_original:str, proc_limpo:str)->None:

        try:
            int(proc_limpo)
        except ValueError as e:
            if 'invalid literal for int' in str(e):
                raise ProcessoForadoPadrao(f'Original: {proc_original}. Limpo: {proc_limpo}')
            else:
                raise e
                
    def __clean_processo(self, proc_num:Union[str, int])->str:

        proc = str(proc_num)
        remove = ('.', '/', '-')

        for char in remove:
            proc = proc.replace(char, '')
        
        self.__assert_processo_clean(proc_num, proc)
        return proc
    

    def __extract_val_empenhado(self, api_resp:dict)->float:

        return api_resp['valEmpenhadoLiquido']
    
    def __extract_val_liquidado(self, api_resp:dict)->float:

        return api_resp['valLiquidado']
    
    def __build_dotacao(self, api_resp:dict)->str:

        orgao = api_resp['codOrgao']
        unidade = api_resp['codUnidade']
        funcao = api_resp['codFuncao']
        subfuncao = api_resp['codSubFuncao']
        programa = api_resp['codPrograma']
        acao = api_resp['codProjetoAtividade']
        elemento = api_resp['codElemento']
        fonte = api_resp['codFonteRecurso'][:2]

        dotacao = f'{orgao}.{unidade}.{funcao}.{subfuncao}.{programa}.{acao}.{elemento}.{fonte}'

        return dotacao

    def __build_col_proxy(self, df:DataFrame, col_empenho:str)->DataFrame:

        df = df.copy()
        try:
            col_proxy = self.__find_col(COL_PROXY, df)
        except ColunaDadosNaoEncontrada:
            col_proxy='proxy'
            df[col_proxy]=np.NaN

        grouped = df[['processo_limpo', col_empenho]].groupby('processo_limpo')
        grouped = grouped.sum().reset_index()

        df[col_proxy] = grouped[col_empenho]

        return df
    
    def __filter_df(self, df:DataFrame)->DataFrame:

        filtro = df['IDRGP'].str.lower().str.contains('sim')

        return df[filtro].copy().reset_index(drop=True)
    
    def pipeline(self, df:DataFrame)->DataFrame:
    
        proc_col = self.__find_col(COL_PROCESSO, df)
        print(proc_col)
        try:
            col_empenho=self.__find_col(COL_EMPENHO, df)
        except ColunaDadosNaoEncontrada:
            col_empenho='empenho liquido'
            df[col_empenho]=np.NaN
        try:
            col_liquidacao=self.__find_col(COL_LIQUIDACAO, df)
        except ColunaDadosNaoEncontrada:
            col_liquidacao='valor_liquidado'
            df[col_liquidacao]=np.NaN

        df=df.copy()
        df = self.__filter_df(df)
        df[COL_DOTACAO]=''
        df['processo_limpo']=''
        df['status_etl'] = ''
        for i, row in df.iterrows():
            print(i)
            try:
                proc = self.__clean_processo(row[proc_col])
                df.loc[i, 'processo_limpo']=proc
                api_resp = self.sof.get_empenho_last_year_by_proc(proc)

                df.loc[i, col_empenho] = self.__extract_val_empenhado(api_resp)
                df.loc[i, col_liquidacao] = self.__extract_val_liquidado(api_resp)
                df.loc[i, COL_DOTACAO] = self.__build_dotacao(api_resp)
                df.loc[i, 'status_etl']='OK'
            except Exception as e:
                if df.loc[i, 'processo_limpo']=='':
                    df.loc[i, 'processo_limpo']=='Fora do padrão'
                df.loc[i, col_empenho] = np.nan
                df.loc[i, col_liquidacao] = np.nan
                df.loc[i, COL_DOTACAO] = ''
                df.loc[i, 'status_etl']=f'Erro: {str(e)}'

        df = self.__build_col_proxy(df, col_empenho)

        return df
    
    def __call__(self, df:DataFrame):

        return self.pipeline(df)


