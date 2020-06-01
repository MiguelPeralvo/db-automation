import os, json, requests

def get_token():
    return os.environ.get('DATABRICKS_TOKEN')

def get_host():
    #host = dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().get("browserHostName").get()
    return os.environ.get('DATABRICKS_HOST')

""" Wrapper for get and post methods for Databricks REST APIs. """
class HttpClient(object):
    def __init__(self, api_prefix, host=None, token=None):
        if not host: host = get_host()
        if not token: token = get_token()
        self.api_uri = os.path.join(host, api_prefix)
        self.token = token

    def _get(self, resource):
        """ Executes an HTTP GET call
        :param resource: Relative path name of resource such as cluster/list
        """
        uri = self._mk_uri(resource)
        rsp = requests.get(uri, headers=self._mk_headers())
        self._check_response(rsp, uri)
        return rsp

    def get(self, resource):
        return json.loads(self._get(resource).text)

    def post(self, resource, data):
        """ Executes an HTTP POST call
        :param resource: Relative path name of resource such as runs/search
        :param data: Post request payload
        """
        uri = self._mk_uri(resource)
        data = json.dumps(data)
        rsp = requests.post(uri, headers=self._mk_headers(), data=data)
        self._check_response(rsp,uri)
        return json.loads(rsp.text)

    def _mk_headers(self):
        return {} if self.token is None else { "Authorization": f"Bearer {self.token}" }

    def _mk_uri(self, resource):
        return f"{self.api_uri}/{resource}"

    def _check_response(self, rsp, uri):
        if rsp.status_code < 200 or rsp.status_code > 299:
            raise Exception(f"HTTP status code: {rsp.status_code}. Reason: {rsp.reason} URL: {uri}")

    def __repr__(self):
        return self.api_uri

class DatabricksHttpClient(HttpClient):
    def __init__(self, host=None, token=None):
        super().__init__("api/2.0", host, token)

class MlflowHttpClient(HttpClient):
    def __init__(self, host=None, token=None):
        super().__init__("api/2.0/mlflow", host, token)