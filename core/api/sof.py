from .client.rest_client import RestClient

from core.utils.date import months_from_today_to, current_month, current_year
from core.exceptions.sof import RespError, EmpenhoInexistente

from config import SOF_API_HOST, SOF_API_TOKEN, SOF_API_VERSION

from typing import List

class ApiSof:

    version = SOF_API_VERSION

    def __init__(self):

        self.client = RestClient(SOF_API_HOST, SOF_API_TOKEN)

    def get_empenhos_by_proc(self, num_proc:int, mes:int, ano:int)->List[dict]:

        api_resp = self.client.get(self.version, 
                                   endpoint='empenhos', 
                                   anoEmpenho=ano, 
                                   mesEmpenho=mes,
                                   numProcesso=num_proc
                                   )
        
        if api_resp['metadados']['txtStatus']!='OK':
            raise RespError(f'Erro na resposta: {api_resp}')
        
        return api_resp['lstEmpenhos']
    

    def get_last_empenho_by_proc(self, num_proc:int)->List[dict]:
        '''Busca no máximo até janeiro do ano fiscal anterior.'''


        ano_anterior = current_year()-1
        jan = 1
        meses_anos = months_from_today_to(jan, ano_anterior)
        for month, year in meses_anos:
            resp = self.get_empenhos_by_proc(num_proc, month, year)
            if resp:
                return resp[0]
        else:
            raise EmpenhoInexistente(f'Não foi encontrado nenhum empenho para o processo {num_proc} até janeiro de {ano_anterior}.')
        
    def get_empenho_last_year_by_proc(self, num_proc:int)->List[dict]:
        '''Busca empenho do ano anterior, iniciando em dezembro até janeiro.'''

        ano_anterior = current_year()-1
        for month in range(12, 0, -1):
            resp = self.get_empenhos_by_proc(num_proc, month, ano_anterior)
            if resp:
                if type(resp) is dict:
                    return resp
                return resp[0]
        else:
            raise EmpenhoInexistente(f'Não foi encontrado nenhum empenho para o processo {num_proc} de dezembro até janeiro de {ano_anterior}.')