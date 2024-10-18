import requests
import json
import sys

from utils import (
    DEV_BACKEND,
    STG_BACKEND,
    DS_BACKEND,
    PROD_BACKEND,
    LOCAL_BACKEND,
)


class DBAPI:
    def __init__(
        self,
        url="",
        userName="",
        passwd="",
        env="",
    ):
        if env == "dev":
            self.url = DEV_BACKEND
        elif env == "stg":
            self.url = STG_BACKEND
        elif env == "test":
            self.url = DS_BACKEND
        elif env == "local":
            self.url = LOCAL_BACKEND
        else:
            self.url = PROD_BACKEND
        self.userName = userName
        self.passwd = passwd
        self.auth_token = self.getAuthToken()
        print("Token authorized")

    def getAuthToken(self):
        # if self.auth_token is not None:
        #     return self.auth_token
        try:
            response = requests.post(
                self.url + "api-token-auth/",
                data={"username": self.userName, "password": self.passwd},
            )
            self.auth_token = json.loads(response.text)["token"]
        except Exception as e:
            raise Exception(e)
        return self.auth_token

    def sendMetaData(self, data={}):
        if self.auth_token is None:
            authToken = self.getAuthToken()
        else:
            authToken = self.auth_token
        try:
            print(self.url + "global_meta/sku/")
            headers = {
                "Authorization": "TOKEN " + authToken,
                "Content-Type": "application/json",
            }
            response = requests.post(
                self.url + "global_meta/sku/",
                headers=headers,
                data=json.dumps(data),
            )
            print("response", response)
            if response.status_code != 201:
                print("Error in API")
                print(response.json())
            # return response.json()
        except Exception as e:
            # raise Exception(e)
            print("exception in API", e)


if __name__ == "__main__":
    obj = DBAPI()
    print(obj.getAuthToken())
