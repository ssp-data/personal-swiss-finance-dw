from dagster import resource, Field
import requests
import os
import datetime as dt


class BekbConnector(object):
    def __init__(self, connection_url, user, password, language, account, api_version):
        self._connection_url = connection_url
        self._user = user
        self._password = password
        self._language = language
        self._account = account
        self._api_version = api_version

        self._login_url = "/login/api/security/" + api_version + "/login/user"
        self._smartlogin_url = "/secure/security/" + api_version + "/login/smartLogin"
        self._secondstep_url = "/secure/authentication/" + api_version + "/secondStep"
        self._headers = {
            'Content-Type': 'application/json;charset=UTF-8',
            'User-Agent': 'PostmanRuntime/7.26.5'
            #'Cookie': 'AL_BALANCE-S=my-super-cookie==; AL_SESS-S=my-super-cookie'
        }

    def smartlogin_connection(self):
        """connects to the BEKB api, logging in with the several steps needed.
        Manually accepting the smartlogin needed in this process"""
        ses = self.get_session()
        url = os.path.join(self._connection_url, self._login_url)

        # payload="{\n    \"passwort\": \"pw\",\n    \"sprache\": \"de\",\n    \"vertrag\": \"000000\"\n}"
        payload = {"passwort": self._password, "sprache": self._language, "vertrag": self._account}

        r = ses.request("POST", url, headers=self._headers, data=payload)
        # print(response.text)
        if self.check_smartlogin(ses) == 200 and self.check_second_step(ses) == 200:
            print("Connection to BEKB E-Banking successful")
            return True
        else:
            print("Connection to BEKB E-Banking failed")
            return False

    def check_smartlogin(self, session: requests.session):
        "checks if connection manually was accepted on the app 'smartlogin'"
        start_time = dt.datetime.now()

        # loop for two minutes or until we have
        response_code = -9
        while start_time + dt.timedelta(0, 120) >= dt.datetime.now() or response_code != 200:
            r = session.request("GET", self._smartlogin_url, headers=self._headers, data={})
            if r.status_code == 200:
                return r.text
        # error case
        return response_code

    def check_second_step(self, session: requests.session):
        "checks after smartlogin again if connection approved"
        r = session.request("GET", self._secondstep_url, headers=self._headers, data={})
        return r.status_code

    def get_session(self):
        "establish authentificated request-session and return it"
        session = requests.Session()
        return session

    def get_main_url(self):
        return self._connection_url


@resource(
    config_schema={
        'connection_url': Field(str),
        'user': Field(str),
        'password': Field(str),
        'account': Field(str),
        'language': Field(str),
        'api_version': Field(str),
    }
)
def bekb_resource(context):
    return BekbConnector(
        context.resource_config['connection_url'],
        context.resource_config['user'],
        context.resource_config['password'],
        context.resource_config['account'],
        context.resource_config['language'],
        context.resource_config['api_version'],
    )
