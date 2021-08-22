from dagster import resource, Field, StringSource
import requests
import os
import datetime as dt
import time
import json


class BekbConnector(object):
    def __init__(self, connection_url, password, language, account, api_version):
        self._session = None
        self._connection_url = connection_url
        self._password = password
        self._language = language
        self._account = account
        self._api_version = api_version

        self._headers = {
            "Content-Type": "application/json;charset=UTF-8",
            "User-Agent": "sspaeti_PDWH/0.1.0",
        }
        self._login_url = os.path.join(
            connection_url, "login/api/security", api_version, "login/user"
        )
        self._smartlogin_url = os.path.join(
            connection_url, "login/api/security", api_version, "login/smartLogin"
        )
        self._secondstep_url = os.path.join(
            connection_url, "secure/api/authentication/v1/secondStep"
        )
        self._logout_url = os.path.join(connection_url, "secure/api/authentication/v1/logout")

    def smartlogin_connection(self) -> requests.Session:
        """connects to the BEKB api, logging in with the several steps needed.
        Manually accepting the smartlogin needed in this process"""
        self.get_new_session()

        payload = {
            "passwort": self._password,
            "sprache": self._language,
            "vertrag": self._account,
        }

        print(f"login to bekb started - {self._login_url}")
        r1 = self._session.request(
            "POST", self._login_url, headers=self._headers, data=json.dumps(payload)
        )

        start_time = dt.datetime.now()
        return_code = -9
        # loop until smartlogin approved access or timed out after x secods
        while return_code != 200 and start_time + dt.timedelta(0, 120) >= dt.datetime.now():
            r2 = self._session.request("GET", self._smartlogin_url, headers=self._headers, data={})
            return_code = r2.status_code
            print(f"c: {r2.status_code} - h: {r2.headers} - url: {self._smartlogin_url}")

            time.sleep(3)
        print("successfully logged in with SmartLogin")

        print(f"verify nessesary second step before fetching data, url: {self._secondstep_url}")
        r3 = self._session.request("GET", self._secondstep_url)
        print("connection sucessful")
        return self._session

    def close_connection(self):
        r99 = self._session.request("GET", self._logout_url)
        self._session.close()

    def fetch_camt053(self, iban, language, date_from, date_to, copy):
        url = (
            os.path.join(self._connection_url, "secure/api/offline-iso/v1/camt053/")
            + str(iban)
            + "?sprache="
            + str(language)
            + "&von="
            + str(date_from)
            + "&bis="
            + str(date_to)
            + "&kopie="
            + str(copy)
        )

        r = self._session.request("GET", url)
        if r.status_code == 200:
            return r.text
        else:
            raise ValueError(
                f"fetch-camt053 failes with status {r.status_code}, text: {r.text}. Url: {url}"
            )

    def get_new_session(self):
        "establish authentificated request-session and return it"
        self._session = requests.Session()

    def get_main_url(self):
        return self._connection_url


@resource(
    config_schema={
        "connection_url": Field(StringSource),
        "password": Field(StringSource),
        "account": Field(StringSource),
        "language": Field(StringSource),
        "api_version": Field(StringSource),
    }
)
def bekb_resource(context):
    return BekbConnector(
        context.resource_config["connection_url"],
        context.resource_config["password"],
        context.resource_config["language"],
        context.resource_config["account"],
        context.resource_config["api_version"],
    )
