import os
from typing import Union
from dotenv import load_dotenv

def copy_dot_env_example():

    if not os.path.exists('.env'):
        print('Definindo o ambiente a partir da cópia do arquivo .env.example.')
        with open('.env.example', 'r') as example:
            with open('.env', 'w') as env_file:
                env_file.write(example.read())



def load_env():

    copy_dot_env_example()
    load_dotenv()

def load_env_var(varname:str)->Union[int, float, str]:

    try:
        return os.environ[varname]
    except KeyError:
        raise RuntimeError(f'Variavel de ambiente {varname} não definida.')
    
def solve_dir(dirname:str)->str:

    if not os.path.exists(dirname):
        os.mkdir(dirname)
    return dirname
    

load_env()

ORIGINAL_DATA_DIR=solve_dir(load_env_var('ORIGINAL_DATA_DIR'))
GENERATED_DATA_DIR=solve_dir(load_env_var('GENERATED_DATA_DIR'))
SHEET_NAME=load_env_var('SHEET_NAME')
SKIP_ROWS=int(load_env_var('SKIP_ROWS'))


SOF_API_TOKEN=load_env_var('SOF_API_TOKEN')
SOF_API_HOST=load_env_var('SOF_API_HOST')
SOF_API_VERSION=load_env_var('SOF_API_VERSION')