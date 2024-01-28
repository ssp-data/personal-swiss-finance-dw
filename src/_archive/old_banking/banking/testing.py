# https://stackoverflow.com/questions/31554771/how-to-use-cookies-in-python-requests
import requests
import time
import os

url1 = "https://banking.bekb.ch/login/api/security/v3/login/user"
password = os.getenv("BEKB_PASSWORD")
login = os.getenv("BEKB_LOGIN")

# payload = '{\n    "passwort": "my-pw",\n    "sprache": "de",\n    "vertrag": "000000"\n}'
payload = (
    '{\n "passwort": "'
    + str(password)
    + '" ,\n  "sprache": "de",\n "vertrag": "'
    + str(login)
    + '"\n}'
)

print(payload)
headers = {
    "Content-Type": "application/json;charset=UTF-8",
    "User-Agent": "sspaeti_python/0.1.0",
}
session = requests.Session()
r1 = session.request("POST", url1, headers=headers, data=payload)
print(f"c: {r1.status_code} - h: {r1.headers}")

# step 2
url2 = "https://banking.bekb.ch/login/api/security/v3/login/smartLogin"
payload = {}
return_code = 401
while return_code != 200:
    r2 = session.request("GET", url2, headers=headers, data=payload)
    return_code = r2.status_code
    print(f"c: {r2.status_code} - h: {r2.headers}")

    time.sleep(3)

# step 3
url3 = "https://banking.bekb.ch/secure/api/authentication/v1/secondStep"
r3 = session.request("GET", url3)
print(r3.status_code)

# step 4 get data
url4 = "https://banking.bekb.ch/secure/api/offline-iso/v1/camt053/CH0500790042490277558?sprache=de&von=2021-07-01&bis=2021-08-10&kopie=true"
r4 = session.request("GET", url4)

print(r4.status_code)
print(r4.text)
