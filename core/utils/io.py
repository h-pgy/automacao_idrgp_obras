import os

def solve_dir(folder:str)->str:

    if not os.path.exists(folder):
        os.mkdir(folder)
    return os.path.abspath(folder)

def solve_path(path:str, parent:str=None)->str:

    if parent:
        parent = solve_dir(parent)
        path = os.path.join(parent, path)
        return path
    
    return os.path.abspath(path)

def file_has_extension(file:str, extension:str)->str:

    if not extension.startswith('.'):
        extension = '.' + extension
    
    return file.endswith(extension)

def list_files_extension(folder:str, extension:str)->str:

    if not os.path.exists(folder):
        raise ValueError(f'Diretorio {folder} inexistente.')
    
    files = []
    for f in os.listdir(folder):
        fpath = solve_path(f, folder)
        if os.path.isfile(fpath) and file_has_extension(fpath, extension):
            files.append(fpath)
        
    return files
