import requests
import re
import json
import datetime
import pickle
from bs4 import BeautifulSoup

import lxml







"""
#find max page
url = 'https://one.viseca.ch/de/transaktionen/550018B7XZ8E7810'
html = requests.get(url)
soup = BeautifulSoup(html.text)
buttons = soup.findAll('button')
p = []
for item in buttons:
    for span in item.contents:
        if len(span.text) <= 3 & len(span.text) != 0:
            p.append(span.text)
if p:
    lastPage = int(p.pop())
else:
    lastPage=1
"""

import requests
from lxml import html
session_requests = requests.session()

payload = {
	"USERNAME": "my-email@bla.com",
	"PASSWORD": "my-password",
	"FORM_TOKEN": "token"
}

login_url = "https://one.viseca.ch/login/login?lang=en"
result = session_requests.get(login_url)

tree = html.fromstring(result.text)
authenticity_token = list(
    set(tree.xpath("//input[@name='FORM_TOKEN']/@value")))[0]


#Step 3: Scrape content
#Now, that we were able to successfully login, we will perform the actual scraping from bitbucket dashboard page

url = 'https://one.viseca.ch/de/transaktionen/550018B7XZ8E7810'
result = session_requests.get(
	url,
	headers=dict(referer=url)
)


#url = 'https://www.immoscout24.ch/en/'+propertyType+'/' + rentOrBuy + '/city-' + city + \
#    '?ci&pn=' + str(i) + '&r='+str(radius) + \
#    '&se=16'  # se=16 means most recent first
#https://www.immoscout24.ch/en/real-estate/buy/city-solothurn?r=40
html = requests.get(url)

soup = BeautifulSoup(html.text)
divs = soup.findAll("div", {"class": "table-row ng-scope"})

hrefs = [item['class'] for item in divs]


#api:
api_url = 'https://api.one.viseca.ch/v1/card/550018B7XZ8E7810/transactions?dateTo=2019-01-09T23:59:59Z&offset=20&pagesize=20&stateType=unknown'



print(hrefs)
