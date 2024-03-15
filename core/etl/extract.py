import pandas as pd

from core.exceptions.xl import SheetNotFound, XlNotFound, MoreThenOneFile
from core.utils.io import solve_path, list_files_extension


class Extract:


    def __init__(self, sheet_name:str, folder:str, **read_params)->None:

        self.read_params = read_params
        self.sheet_name = sheet_name
        self.folder = folder

    def __load_xl_file(self, folder:str, **read_params):

        files = list_files_extension(folder, '.xls')
        files.extend(list_files_extension(folder, '.xlsx'))
        
        if len(files)>1:
            raise MoreThenOneFile(f'Mais de um arquivo excel: {files}')
        if len(files)<1:
            raise XlNotFound(f'Nenhum arquivo de excel encontrado no folder: {folder}')

        return pd.read_excel(files[0], **read_params)


    def __capture_load_sheet_error(self, sheet_name:str, e:Exception)->None:

        error_msg  = f"Worksheet named '{sheet_name}' not found"
        if str(e)==error_msg:
            raise SheetNotFound(error_msg)
        else:
            raise e
        

    def __load_sheet(self, sheet_name:str, folder:str, **read_params)->pd.DataFrame:

        try:
            return self.__load_xl_file(folder, sheet_name=sheet_name, **read_params)
        except ValueError as e:
            self.__capture_load_sheet_error(sheet_name, e)

    def __call__(self)->pd.DataFrame:

        return self.__load_sheet(self.sheet_name, self.folder, **self.read_params)
            