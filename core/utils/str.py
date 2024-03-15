def remover_acentos(name):
    
    acento_letra = {
        'ç' : 'c',
        'á' : 'a',
        'â' : 'a',
        'à' : 'a',
        'ã' : 'a',
        'ä' : 'a',
        'é' : 'e',
        'ê' : 'e',
        'è' : 'e',
        'ë' : 'e',
        'í' : 'i',
        'î' : 'i',
        'ì' : 'i',
        'ï' : 'i',
        'ó' : 'o',
        'ô' : 'o',
        'ò' : 'o',
        'ø' : 'o',
        'õ' : 'o',
        'ö' : 'o',
        'ú' : 'u',
        'û' : 'u',
        'ù' : 'u',
        'ü' : 'u',
        'ñ' : 'n',
        'ý' : 'y'
    }
    
    chars = list(name)
    
    return ''.join([acento_letra.get(char, char) for char in chars])