import re
from typing import Union, List, Tuple, Literal
from core.exceptions.sof import ProcessoForadoPadrao, RespError

from config import PROC_REGEX_PATT


class ExtractApiVal:

    key_empenho = 'valEmpenhadoLiquido'
    key_liquidacao = 'valLiquidado'
    
    def __extract_val(self, api_resp:dict, key:str, enforce_num=True)->Union[str, float]:

        try:
            val = api_resp[key]
        except KeyError:
            raise RespError(f'Erro na resposta do SOF. Chave {key} nao encontrada')
        
        if enforce_num:
            try:
                val = float(val)
            except (ValueError, TypeError):
                raise RespError(f'Erro na resposta do SOF para a chave {key}. Não foi possível converter para número: {val}')
        
        return val


    def __extract_val_empenhado(self, api_resp:dict)->float:

        return self.__extract_val(api_resp, self.key_empenho, True)
        
        
    def __extract_val_liquidado(self, api_resp:dict)->float:

        return self.__extract_val(api_resp, self.key_liquidacao, True)

    def __build_dotacao(self, api_resp:dict)->str:

        orgao = self.__extract_val(api_resp, 'codOrgao', False)
        unidade = self.__extract_val(api_resp, 'codUnidade', False)
        funcao = self.__extract_val(api_resp, 'codFuncao', False)
        subfuncao = self.__extract_val(api_resp, 'codSubFuncao', False)
        programa = self.__extract_val(api_resp, 'codPrograma', False)
        acao = self.__extract_val(api_resp, 'codProjetoAtividade', False)
        elemento = self.__extract_val(api_resp, 'codElemento', False)
        fonte = self.__extract_val(api_resp, 'codFonteRecurso', False)[:2]

        dotacao = f'{orgao}.{unidade}.{funcao}.{subfuncao}.{programa}.{acao}.{elemento}.{fonte}'

        return dotacao
    
    def __solve_extraction(self, api_resp:dict, tipo=Literal['empenho', 'liquidacao', 'dotacao'])->Union[float, str]:

        if tipo == 'empenho':
            return self.__extract_val_empenhado(api_resp)
        if tipo == 'liquidacao':
            return self.__extract_val_liquidado(api_resp)
        if tipo == 'dotacao':
            return self.__build_dotacao(api_resp)

    def __call__(self, api_resp:dict, tipo=Literal['empenho', 'liquidacao', 'dotacao'])->Union[float, str]:

        tipos = {'empenho', 'liquidacao', 'dotacao'}
        if tipo not in tipos:
            raise NotImplementedError(f'Tipo nao disponivel. Disponiveis: {tipos}')
        
        return self.__solve_extraction(api_resp, tipo)

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
                raise ProcessoForadoPadrao(f'Erro: processo fora do Padrão. Original: {proc_original}. Limpo: {proc_limpo}')
            else:
                raise e
                
    def __clean_processo(self, proc_num:Union[str, int])->str:

        proc = str(proc_num)
        remove = ('.', '/', '-')

        for char in remove:
            proc = proc.replace(char, '')
        
        self.__assert_processo_clean(proc_num, proc)
        return proc

    def __pipeline(self, proc_num:str)->List[str]:

        proc_num = str(proc_num)
        parsed_raw = self.__parse(proc_num)
        extracted = self.__extract_from_regex(parsed_raw)

        parsed_clean = [self.__clean_processo(proc) for proc in extracted]

        if len(parsed_clean) < 1:
            raise ProcessoForadoPadrao(f'Erro: nenhum processo no formato encontrado. Valor original: {proc_num}')

        return parsed_clean
    

    def __call__(self, proc_num:str, raise_for_errors=False)->List[str]:

        try:
            return self.__pipeline(proc_num)
        except Exception as e:
            if raise_for_errors:
                raise e
            else:
                return str(e)