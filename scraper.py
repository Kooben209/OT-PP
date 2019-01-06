import scraperwiki
import sqlite3
import logging
from   bs4 import BeautifulSoup
import requests
import time
import sys
import os
from decimal import Decimal
from re import sub
from datetime import datetime, timedelta
import urllib.parse as urlparse
import math
import re
import json


timestr = time.strftime("%d%m%Y-%H%M%S")
currentScriptName=os.path.basename(__file__)

def createStore():
	scraperwiki.sqlite.execute("CREATE TABLE IF NOT EXISTS 'otmdata' ( 'propId' TEXT, link TEXT, title TEXT, address TEXT, price BIGINT, 'displayPrice' TEXT, image1 TEXT, 'pubDate' DATETIME, 'addedOrReduced' DATE, reduced BOOLEAN, location TEXT, CHECK (reduced IN (0, 1)), PRIMARY KEY('propId'))")
	scraperwiki.sqlite.execute("CREATE UNIQUE INDEX IF NOT EXISTS 'otmdata_propId_unique' ON 'otmdata' ('propId')")

def saveToStore(data):
	scraperwiki.sqlite.execute("CREATE TABLE IF NOT EXISTS 'otmdata' ( 'propId' TEXT, link TEXT, title TEXT, address TEXT, price BIGINT, 'displayPrice' TEXT, image1 TEXT, 'pubDate' DATETIME, 'addedOrReduced' DATE, reduced BOOLEAN, location TEXT, CHECK (reduced IN (0, 1)), PRIMARY KEY('propId'))")
	scraperwiki.sqlite.execute("CREATE UNIQUE INDEX IF NOT EXISTS 'otmdata_propId_unique' ON 'otmdata' ('propId')")
	scraperwiki.sqlite.execute("INSERT OR IGNORE INTO 'otmdata' VALUES (?,?,?,?,?,?,?,?,?,?,?)", (data['propId'], data['link'], data['title'], data['address'], data['price'], data['displayPrice'], data['image1'], data['pubDate'], data['addedOrReduced'], data['reduced'], data['location']))

def parseAskingPrice(aPrice):
	try:
		value = round(Decimal(sub(r'[^\d.]', '', aPrice)))
	except:
		value = 0
	return value

filtered_dict = {k:v for (k,v) in os.environ.items() if 'MORPH_URL' in k}

excludeAgents = []
if os.environ.get("MORPH_EXCLUDE_AGENTS") is not None:
	excludeAgentsString = os.environ["MORPH_EXCLUDE_AGENTS"]
	excludeAgents = excludeAgentsString.lower().split("^")

keywords = []
	
if os.environ.get("MORPH_KEYWORDS") is not None:
	keywordsString = os.environ["MORPH_KEYWORDS"]
	keywords = keywordsString.lower().split("^")
	
sleepTime = 5

if os.environ.get("MORPH_SLEEP") is not None:
	sleepTime = int(os.environ["MORPH_SLEEP"])

if os.environ.get("MORPH_DOMAIN") is not None:
	domain = os.environ["MORPH_DOMAIN"]
	
if os.environ.get("MORPH_FIRST_RUN") is not None:
	if os.environ.get('MORPH_FIRST_RUN') == "1":
		createStore()
	
	
with requests.session() as s:
	s.headers['user-agent'] = 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/64.0.3282.186 Safari/537.36'
	
	#Loop through location URLs
	totalNoProbates=0
	for k, v in filtered_dict.items():
		location = k.replace("MORPH_URL_","").replace("_"," ").title()
		checkURL = v
		if os.environ.get('MORPH_DEBUG') == "1":
			print(checkURL)
			
		if os.environ.get('MORPH_MAXDAYS') == "0":
			checkURL = checkURL.replace("&recently-added=24-hours","")
			
		parsedURL = urlparse.urlparse(checkURL)
		params = urlparse.parse_qs(parsedURL.query,keep_blank_values=1)
			
		#Get first page of results
		print('Requesting results for '+location)
		r1 = s.get(checkURL)
		soup = BeautifulSoup(r1.content, 'html.parser')
		
		pageSize=24
		try:
			pageSettings = soup.find("script", attrs={'src': None},string=re.compile("dataLayer ="))
			if pageSettings is not None:
				pageSettings = pageSettings.text.replace('dataLayer = [','').replace('];','')
				pageSettingsDic=json.loads(pageSettings)
				pageSize = pageSettingsDic['frame-size']
		except:
			pageSize = pageSize

		
		try:
			numOfResults = soup.find("span", {"class" : "results-count"}).text.replace(" result", "").replace("s", "")
			numOfResults = int(numOfResults)
			numOfPages = math.ceil(float(numOfResults)/pageSize)
		except:
			numOfResults = 0	
			numOfPages = 0	
		page = 1
		
		print('numOfResults= '+str(numOfResults))
		print('numOfPages= '+str(numOfPages))
		
		noProbates=0
		while page <= numOfPages:
			
			if page > 1: #get next page
				time.sleep(sleepTime)
				if not params: #has NO querystring params
					nextPageURL = checkURL+"?page="+str(page-1)					
				else:
					nextPageURL = checkURL+"&page="+str(page-1)
					
				print('requesting next page')
				if os.environ.get('MORPH_DEBUG') == "1":
					print(nextPageURL)
				r1 = s.get(nextPageURL)
				soup = BeautifulSoup(r1.content, 'html.parser')
			#Loop over and visit each result and check if probate
			adverts = soup.findAll("li", {"class" : "result property-result panel"})
			for advert in adverts:
				if os.environ.get('MORPH_DEBUG') == "1":
					print('searching through results')

				resultLink = advert.find("span", {"class" : "title"}).a['href']
				
				#get individual result page
				time.sleep(sleepTime)
				if os.environ.get('MORPH_DEBUG') == "1":
					print('requesting result '+domain+resultLink)

				r1 = s.get(domain+resultLink)
				soup = BeautifulSoup(r1.content, 'html.parser')
				advertDesc = str(soup.find("div", {"class" : "panel-content description-tabcontent"}))
				
				if any(x in advertDesc for x in keywords): #check if probate
					noProbates +=1
					advertMatch = {}
					reduced=False
					addedOrReduced = datetime.now().date()
					advert = soup.find("div", {"id" : "details-results"})
					propId = advert["data-property-id"]
					
					details = soup.find("div", {"class" : "details-heading"})
					title = details.h1.text
					address = details.find("p", {"class" : ""}).text
					propLink = soup.find("meta", {"property" : "og:url"})['content']
					image1 = soup.find("meta", {"property" : "og:image"})['content']
					price = parseAskingPrice(soup.find("span", {"class" : "price-data"}).text.strip())
					displayPrice = soup.find("span", {"class" : "price-data"}).text.strip()

					#Build up save to store
					advertMatch['propId'] = propId
					advertMatch['link'] = propLink
					advertMatch['title'] = title
					advertMatch['address'] = address
					advertMatch['price'] = price
					advertMatch['displayPrice'] = displayPrice
					advertMatch['image1'] = image1
					advertMatch['pubDate'] = datetime.now()
					advertMatch['addedOrReduced'] = addedOrReduced
					advertMatch['reduced'] = reduced
					advertMatch['location'] = location
					
					saveToStore(advertMatch)
			page +=1
		totalNoProbates = totalNoProbates+noProbates
		print('Found '+str(noProbates)+' Probate Properties in this search of '+location)
print('Found a total of '+str(totalNoProbates)+' Probate Properties in this run')
sys.exit()
