#Ingest data from API
import requests

class extract:
    def __init__(self,url):
        self.url=url

    def ingest(self):

        try:
            response=requests.get(self.url)

        except Exception as e:
            print(f"unable to extract data with the error {e}")
        else:
            print("Data extraction is complete")

        return response.json()




