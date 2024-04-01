import re
from typing import Union, List, Tuple
from core.exceptions.sof import ProcessoForadoPadrao

from config import PROC_REGEX_PATT

def build_dotacao(api_resp:dict)->str:

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

class ParseProc:

    proc_patt = PROC_REGEX_PATT

    def __parse(self, proc_string:str)->List[Tuple[str]]:

        return re.findall(self.proc_patt, proc_string)
    
    def __extract_from_regex(self, parsed_response:List[Tuple[str]])->List[str]:

        parsed = []

        for tupla in parsed_response:
            for item in tupla:
                if item != '':
                    parsed.append(item)

        return parsed


    def __assert_processo_clean(self, proc_original:str, proc_limpo:str)->None:

        try:
            int(proc_limpo)
        except ValueError as e:
            if 'invalid literal for int' in str(e):
                raise ProcessoForadoPadrao(f'Processo fora do Padrão. Original: {proc_original}. Limpo: {proc_limpo}')
            else:
                raise e
                
    def __clean_processo(self, proc_num:Union[str, int])->str:

        proc = str(proc_num)
        remove = ('.', '/', '-')

        for char in remove:
            proc = proc.replace(char, '')
        
        self.__assert_processo_clean(proc_num, proc)
        return proc
    
    def __call__(self, proc_num:str)->List[str]:

        parsed_raw = self.__parse(proc_num)
        extracted = self.__extract_from_regex(parsed_raw)

        parsed_clean = [self.__clean_processo(proc) for proc in extracted]

        return parsed_clean