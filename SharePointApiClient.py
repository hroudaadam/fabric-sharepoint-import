import requests

class HttpClient:
    def send(self, method, url, headers={}, body={}):
        response = requests.request(method, url, headers=headers, data=body)
        response.raise_for_status()
        return response.json()

class SharePointApiClient:
    def __init__(self, sharepoint_domain, tenant_id, client_id, client_secret):
        self.__http_client = HttpClient()
        
        self.__sharepoint_domain = sharepoint_domain
        self.__tenant_id = tenant_id
        self.__client_id = client_id
        self.__client_secret = client_secret
        
        self.__access_token = ""

    def __get_api_url(self, site_name, relative_url):
        return f"https://{self.__sharepoint_domain}.sharepoint.com/sites/{site_name}/_api" + relative_url

    def __send_request(self, method, url, body = {}):
        if self.__access_token == "": self.__get_access_token()

        headers = {
            "Authorization": self.__access_token,
            "Accept": "application/json; odata=nometadata"
        }

        response = self.__http_client.send(method, url, headers, body)
        return response

    def __get_access_token(self):
        url = f"https://accounts.accesscontrol.windows.net/{self.__tenant_id}/tokens/OAuth/2"
        headers = {'Content-Type': 'application/x-www-form-urlencoded'}
        body = {
            "grant_type": "client_credentials",
            "client_id": f"{self.__client_id}@{self.__tenant_id}",
            "client_secret": client_secret,
            "resource": f"00000003-0000-0ff1-ce00-000000000000/{self.__sharepoint_domain}.sharepoint.com@{self.__tenant_id}"
        }
    
        response = self.__http_client.send("POST", url, headers, body)
        self.__access_token = "Bearer " + response["access_token"]

    def get_all_list_items(self, site_name, list_id, filter="", select="", expand="", top=5000):
        url = self.__get_api_url(
            site_name, 
            f"/web/lists(guid'{list_id}')/items?$select={select}&$expand={expand}&$top={top}"
        )
        
        data_pages = []
        fetch_next = True

        # iterate pages
        while fetch_next:
            response = self.__send_request("GET", url)
            data_pages.append(response["value"])
            url = response.get("odata.nextLink")
            fetch_next = url != None          

        # flatten
        return [item for page in data_pages for item in page]

    def get_list_schema(self, site_name, list_id):
        url = self.__get_api_url(
            site_name,
            f"/web/lists(guid'{list_id}')/fields?$select=Title,InternalName,TypeAsString,TypeDisplayName,Required&filter=Hidden eq false"
        )       
        response = self.__send_request("GET", url)
        return response['value']