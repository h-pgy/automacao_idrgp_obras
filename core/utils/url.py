

class UrlBuilder:
    '''Builds url for request.'''


    def __init__(self, domain: str):

        self.domain = 'https://' + self.slash_ending(domain)

    def slash_ending(self, slug : str)->str:

        if not slug.endswith('/'):
            slug = slug + '/'

        return slug

    def build_params(self, params: dict)->str:
    
        params = [f'{key}={val}' for key, val in params.items()]
        
        params = '&'.join(params)
        
        return '?'+params


    def build_url(self, namespace: str, endpoint: str, **params)->str:
        
        #apenas o namespace precisa de slash, o endpoint nao
        namespace = self.slash_ending(namespace)

        url = self.domain + namespace + endpoint
        
        if params:
            params = self.build_params(params)
            url = url + params
        
        return url

    def __call__(self, namespace, endpoint, **params):

        return self.build_url(namespace, endpoint, **params)