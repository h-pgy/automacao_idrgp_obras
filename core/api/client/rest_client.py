from requests import Session
from typing import Union

from core.utils.url import UrlBuilder

class RestClient:

    def __init__(self, domain:str, token:Union[str, None])->None:

        self.session = Session()
        self.token = token
        self.domain = domain
        self.build_url = UrlBuilder(domain)

        if self.token:
            self.__set_token()

    def __set_token(self)->None:

        self.session.headers.update({'Authorization' : f'Bearer {self.token}'})


    def get(self, namespace:str, endpoint:str, **query_params)->Union[dict, list]:

        url = self.build_url(namespace, endpoint, **query_params)
        with self.session.get(url) as r:
            if r.status_code!=200:
                raise RuntimeError(f'Falha na requisição. Status: {r.status_code}. Message: {r.text}')
            
            return r.json()
    
    