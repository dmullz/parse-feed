# Standard library imports
import calendar
import dateutil.parser as parser
from datetime import datetime, timedelta, timezone
import re
import time
import os
import json

# Third party imports
import feedparser
import requests

# env variable for logging
env = ""
		
def classify_text(nlu_url, nlu_apikey, classify_model, text):

	URL = nlu_url + "/v1/analyze?version=2022-04-07"
	headers = {"Content-Type":"application/json"}
	data = {"text":text,
			"features":{
				"classifications":{
					"model":classify_model}}}
	result_map = {}
	try:
		r = requests.post(URL, auth=("apikey",nlu_apikey), headers=headers, json=data)
		r.raise_for_status()
		for class_found in r.json()["classifications"]:
			result_map[class_found['class_name']] = class_found['confidence']
		return result_map
	except Exception as ex:
		print("*** " + env + " ERROR GETTING NLC SCORE:", str(ex))
		#print("*** " + env + " PROBLEM SENDING DATA:", data)
		return None
	

def translate_text(url, translate_apikey, language, text):

	if "en" in language or "unk" in language:
		return text
		
	language_mapping = {
		"ger": "DE",
		"de-DE": "DE",
		"it-IT": "IT",
		"fr-FR": "FR",
		"es-ES": "ES",
		"NL": "NL",
		"nl": "NL",
		"fr": "FR",
		"de": "DE",
		"it": "IT",
		"es": "ES"
	}
	
	if language not in language_mapping:
		return text
	
	data = {
		"auth_key": translate_apikey,
		"text": text,
		"source_lang": language_mapping[language],
		"target_lang": "EN-US"
	}
	
	try:
		r = requests.get(url, params=data)
		
		r.raise_for_status()
		return r.json()["translations"][0]["text"]
	except Exception as e:
		print("*** " + env + " ERROR TRANSLATING TEXT:", str(e))
		return ""


# @DEV: Takes a date string and converts it to central time stamp in miliseconds
# @PARAM: _date is a string in the form of: "Mon, 20 May 2019 18:00:56 +0000"
def get_UTC_time(_date):
	utc_in_miliseconds = calendar.timegm(parser.parse(_date).timetuple()) * 1000
	return utc_in_miliseconds


# @DEV: Uses the feedparser library to extract all article URLs from an XML feed and return as a list.
# @PARAM: _feed_list is a list of feeds to parse.
def parse_feed(_nlu_url,_nlu_api_key,_classify_id,_financial_classify_id, _todays_date_pretty, _todays_date_struct, _already_ingested, _use_sql, translate_url, translate_apikey, _feed_list=[]):
	article_map = {}
	today = datetime.now()
	today_utc = today.replace(tzinfo = timezone.utc)
	today_utc_milli = int(today_utc.timestamp() * 1000)

	for feed in _feed_list:
		start_time = time.perf_counter()
		data = None
		try:
			data = feedparser.parse(feed['feed_url'])
			if data.status == 403:
				raise Exception("Cannot access RSS Feed: (403 Forbidden for URL)")
			if data.status == 401:
				raise Exception("Cannot access RSS Feed: (401 Unauthorized)")
			if len(data.entries) == 0:
				raise Exception("No entries found in parsed feed")
		except Exception as ex:
			print("*** " + env + " ERROR PARSING FEED: " + feed['feed_name'] + " FROM URL: " + feed['feed_url'], str(ex))
			continue
		article_count = 0
		for item in data.entries:
			yesterday = datetime.now() - timedelta(days = 3)
			yesterday_utc = yesterday.replace(tzinfo = timezone.utc)
			yesterday_utc_milli = int(yesterday_utc.timestamp() * 1000)
			tomorrow = datetime.now() + timedelta(days = 1)
			tomorrow_utc = tomorrow.replace(tzinfo = timezone.utc)
			tomorrow_utc_milli = int(tomorrow_utc.timestamp() * 1000)
			language = feed['language']
			article_title = ""
			
			if hasattr(item, 'title'):
				article_title = re.sub(r'[^\w\d\s\.\,\-\']','',item.title)		
			else:
				continue
				
			article_count += 1
			if article_count > 100:
				break
				
			if _use_sql:
				# Ensure Publish date is within 24 hours (past or future) of now, otherwise skip
				if (hasattr(item, 'published') and (get_UTC_time(item.published) > yesterday_utc_milli and get_UTC_time(item.published) < tomorrow_utc_milli)) or not hasattr(item, 'published'):
					# Translate title
					tt_start = time.perf_counter()
					article_title = translate_text(translate_url, translate_apikey, language, article_title)
					if env == 'DEV':
						tt_end = time.perf_counter()
						print("*** " + env + " TIME ELAPSED TRANSLATING TITLE: ", article_title, str(tt_end - tt_start))
					if article_title == "":
						continue
					
					# Skip already ingested articles
					skip = False
					#print("*** " + env + " CHECKING FEED AGAINST ALREADY INGESTED: ", feed['feed_name'], " NUMBER:", len(_already_ingested[feed['feed_name']]))
					for ingested_article in _already_ingested[feed['feed_name']]:
						if strip_characters(article_title.lower()) == strip_characters(ingested_article['article_title'].lower()):
							#print("*** " + env + " SKIPPING ITEM VS DB: ", item.title, " VS ", ingested_article['article_title'])
							skip = True
							break
					if skip:
						if env == 'DEV':
							print("*** " + env + " SKIPPING ARTICLE USING DB: ", article_title, " FEED:", feed['feed_url'])
						continue
				else:
					if env == 'DEV':
						print("*** " + env + " SKIPPING ARTICLE DUE TO BAD PUBLISH DATE: ", article_title, " FEED:", feed['feed_url'])
					continue
			else:
				print("*** " + env + " NOT USING DB")
				# Ensure Publish date is within 24 hours (past or future) of now, otherwise skip
				if (hasattr(item, 'published') and (get_UTC_time(item.published) < yesterday_utc_milli or get_UTC_time(item.published) > tomorrow_utc_milli)) or not hasattr(item, 'published'):
					print("*** " + env + " SKIPPING ARTICLE DUE TO BAD PUBLISH DATE: ", article_title, "FEED:", feed['feed_url'])
					continue
				if (hasattr(item, 'published') and (get_UTC_time(feed['last_updated_date']) > get_UTC_time(item.published))) or not hasattr(item, 'published'):
					print("*** " + env + " SKIPPING ARTICLE USING TIME: ", article_title, "FEED:", feed['feed_url'])
					continue
				
				# Translate Title
				article_title = translate_text(translate_url, translate_apikey, language, article_title)
				if article_title == "":
					continue
				
			#find a file name
			split_url = item.link.split('/')
			counter = -1
			file_name = split_url[counter]
			while not file_name:
				counter -= 1
				file_name = split_url[counter]

			if filter_by_title(file_name, True):
				
				ct_start = time.perf_counter()
				class_map = {}
				if "Dow Jones" in feed['publisher']:
					if financial_blacklist(article_title):
						continue
					else:
						class_map = classify_text(_nlu_url, _nlu_api_key, _financial_classify_id, article_title)
				elif "Arena Group" in feed['publisher'] and "TheStreet" in feed['feed_name']:
					class_map = classify_text(_nlu_url, _nlu_api_key, _financial_classify_id, article_title)
				else:
					class_map = classify_text(_nlu_url, _nlu_api_key, _classify_id, article_title)
				if env == 'DEV':
						ct_end = time.perf_counter()
						print("*** " + env + " TIME ELAPSED TRANSLATING TITLE: ", article_title, str(ct_end - ct_start))
				
				if not class_map:
					negative_classifier = 1.0
					lead_classifier = 0.0
				else:
					negative_classifier = class_map['NEGATIVE']
					lead_classifier = class_map['LEAD']

				if not hasattr(item, 'published') or (hasattr(item, 'published') and get_UTC_time(item.published) > today_utc_milli):
					article_map[file_name] = {
						"metadata": {
							"url":item.link,
							"pub_date": today_utc_milli,
							"language": language,
							"title":article_title, 
							"publisher":feed['publisher'], 
							"feed_name":feed['feed_name'],
							"negative_classifier": negative_classifier,
							"lead_classifier": lead_classifier
						}
					}      
				else:
					article_map[file_name] = {
						"metadata": {
							"url":item.link,  
							"pub_date": get_UTC_time(item.published),
							"language": language,
							"title":article_title, 
							"publisher":feed['publisher'], 
							"feed_name":feed['feed_name'],
							"negative_classifier": negative_classifier,
							"lead_classifier": lead_classifier
						}
					}
				
				if "content" in item:
					article_map[file_name]["metadata"]["article_text"] = item.content[0].value
				if "Dow Jones" in feed['publisher']:
					if "advisor/articles" in item.link:
						article_map[file_name]["metadata"]["lead_classifier"] = 1.0
				
				if "Engadget" in feed['feed_name']:
					article_map[file_name]["metadata"]["article_text"] = item.description
		
		end_time = time.perf_counter()
		print("*** " + env + " TIME ELAPSED PARSING FEED: ", feed['feed_url'], str(end_time - start_time))
	return {"article_map" : article_map }
	
	
# @DEV: Return True if title contains blacklist terms, false otherwise

def financial_blacklist(title):
	blacklist_regexes = [r"crossword",r"pepper.*and\ssalt",r"firm\sretention\ssummary",r"corrections.*amplifications"]
	for r in blacklist_regexes:
		if re.search(r,title.lower()):
			print("*** " + env + " SKIPPING ARTICLE USING FINANCIAL BLACKLIST: ", title)
			return True
	return False

# @DEV: Filter the articles by their title. It should be passed a title and a Boolean to use a swear word filter or not. 
# It will return True or False (False if it should be filtered out, True if not).
def filter_by_title(title, swear_flag):
	# OLD HARD FILTER
	#filtered_regexes = [r"\bshop",r"\bsale",r"\bphoto",r"\bprice","weed","marijuana","cannabis"]
	#for r in filtered_regexes:
	#	if re.search(r,title):
	#		print("*** " + env + " SKIPPING ARTICLE USING FILTER: ", title)
	#		return False

	if swear_flag:
		SWEAR_LIST = ["4r5e","5h1t","5hit","a55","ar5e","arrse","ass-fucker","assfucker","assfukka","asshole","assholes","asswhole","b!tch","b00bs","b17ch","b1tch","ballbag","ballsack","beastial","beastiality","bellend","bestial","bestiality","blow job","blowjob","blowjobs","boiolas","bollock","bollok","buceta","bunny fucker","butthole","buttmuch","c0ck","c0cksucker","carpet muncher","cawk","chink","cipa","cnut","cock-sucker","cockface","cockhead","cockmunch","cockmuncher","cocks","cocksuck","cocksucked","cocksucker","cocksucking","cocksucks","cocksuka","cocksukka","cok","cokmuncher","coksucka","coon","crap","cumshot","cunilingus","cunillingus","cunnilingus","cunt","cuntlick","cuntlicke ","cuntlicking","cunts","cyalis","cyberfuc","cyberfuck","cyberfucked","cyberfucker","cyberfuckers","cyberfucking","d1ck","dickhead","dink","dinks","dirsa","dlck","dog-fucker","doggin","dogging","donkeyribber","doosh","duche","f u c k","f u c k e r","f4nny","fag","fagging","faggitt","faggot","faggs","fagot","fagots","fags","fannyflaps","fannyfucker","fanyy","fcuk","fcuker","fcuking","feck","fecker","felching","fellate","fingerfuck","fingerfucked","fingerfucker","fingerfuckers","fingerfucking","fingerfucks","fistfuck","fistfucked","fistfucker","fistfuckers","fistfucking","fistfuckings","fistfucks","flange","fook","fooker","fuck","fucka","fucked","fucker","fuckers","fuckhead","fuckheads","fuckin","fucking","fuckings","fuckingshitmotherfucker","fuckme","fucks","fuckwhit","fuckwit","fudge packer","fudgepacker","fuk","fuker","fukker","fukkin","fuks","fukwhit","fukwit","fux","fux0r","f_u_c_k","gangbang","gangbanged","gangbangs","gaylord","gaysex","goatse","god-dam","god-damned","hardcoresex ","heshe","hoar","hoare","hoer","hore","horniest","hotsex","jap","kawk","knobead","knobed","knobend","knobhead","knobjocky","knobjokey","kunilingus","l3i+ch","l3itch","mothafuck","mothafucka","mothafuckas","mothafuckaz","mothafucked","mothafucker","mothafuckers","mothafuckin","mothafucking ","mothafuckings","mothafucks","mother fucker","motherfuck","motherfucked","motherfucker","motherfuckers","motherfuckin","motherfucking","motherfuckings","motherfuckka","motherfucks","muff","mutha","muthafecker","muthafuckker","muther","mutherfucker","n1gga","n1gger","nigg3r","nigg4h","nigga","niggah","niggas","niggaz","nigger","niggers","nob jokey","nobhead","nobjocky","nobjokey","numbnuts","nutsack","pecker","penisfucker","phonesex","phuck","phuk","phuked","phuking","phukked","phukking","phuks","phuq","pigfucker","pimpis","pisser","pissers","pisses","pissflaps","pissin","pissing","pissoff","pricks","retard","rimjaw","rimming","s hit","s.o.b.","schlong","scroat","scrote","scrotum","sh!+","sh!t","sh1t","shagger","shaggin","shagging","shemale","shi+","shit","shitdick","shite","shited","shitey","shitfuck","shitfull","shithead","shiting","shitings","shits","shitted","shitter","shitters","shitting","shittings","shitty","skank","slut","sluts","smegma","son-of-a-bitch","spac","spunk","s_h_i_t","testical","titfuck","tittie5","tittiefucker","tittyfuck","tittywank","titwank","tosser","tw4t","twat","twathead","twatty","twunt","twunter","w00se","whoar","whore","willies"]
		if any([element in title.split('-') for element in SWEAR_LIST]):
			print("*** " + env + " SKIPPING ARTICLE USING SWEAR FILTER: ", title)
			return False
				
	return True


# @DEV: Connect to SQL DB to get articles ingested in last 24 hours
# @RET: Returns True if SQL query was successful, and returns object containing all articles ingested
def get_ingested_articles(feed_list, url, apikey):
	yesterday = datetime.now() - timedelta(days = 1)
	yesterday_utc = yesterday.replace(tzinfo = timezone.utc)
	yesterday_utc_milli = int(yesterday_utc.timestamp() * 1000)
	past_day = datetime.now() - timedelta(days = 7)
	past_day_utc = past_day.replace(tzinfo = timezone.utc)
	past_day_utc_formatted = past_day_utc.strftime("%Y-%m-%d")
	ingested_articles = {}
	for feed in feed_list:
		params = {"apikey": apikey, "ingestdate": past_day_utc_formatted, "magazine": feed['feed_name']}
		try:
			r = requests.get(url + 'v2/get-article-by-ingestdate-magazine', params=params)
			r.raise_for_status()
			ingested_articles[feed['feed_name']] = r.json()
		except Exception as ex:
			print("*** " + env + " ERROR USING SQL DB ***", str(ex))
			return False, {}
	return True, ingested_articles
	
# @DEV: Strips string of all non-letter or number characters
# @RET: replaced string
def strip_characters(title):
	return re.sub(r'[^\w\d]','',title)


def main(_param_dictionary):
	global env
	inputs = os.environ
	env = inputs['env']
	
	for feed_item in _param_dictionary['feed_list']:
		print("**** " + env + " **** PARSING FEED: ", feed_item['feed_name'])
	
	# We will use today's date as the pub_date
	todays_date_struct = time.strptime(datetime.today().strftime('%a, %d %b %Y'),'%a, %d %b %Y')
	todays_date_pretty = time.strftime('%a, %d %b %Y', todays_date_struct)
	
	start_time = time.perf_counter()
	use_sql, already_ingested = get_ingested_articles(_param_dictionary['feed_list'],inputs['sql_db_url'],inputs['sql_db_apikey'])
	#print("**** " + env + " **** ALREADY INGESTED ARTICLES", already_ingested)
	end_time = time.perf_counter()
	print("*** " + env + " TIME ELAPSED GATHERING INGESTED ARTICLES: ", str(end_time - start_time))
	
	
	parsed_feed_map = parse_feed(
		_nlu_url = inputs['sentiment_url'],
		_nlu_api_key = inputs['sentiment_apikey'],
		_classify_id = inputs['nlc_id'],
		_financial_classify_id = inputs['financial_classify_id'],
		_feed_list=_param_dictionary['feed_list'],     
		_todays_date_struct = todays_date_struct,
		_todays_date_pretty = todays_date_pretty,
		_already_ingested = already_ingested,
		_use_sql = use_sql,
		translate_url = inputs["translate_url"],
		translate_apikey = inputs["translate_apikey"]
		)

	parsed_feed = parsed_feed_map['article_map']
	
	URL = inputs["download_upload_url"]
	headers = {"Content-Type":"application/json"}
	data = {
		'parsed_feed': parsed_feed
	}
	#time_out = 5
	#attempts = 1
	#while True:
	#	try:
	#		r = requests.post(URL, headers=headers, json=data)
	#		r.raise_for_status()
	#	except Exception as ex:
	#		if attempts > 2:
	#			print("*** " + env + " ERROR CALLING DOWNLOAD-UPLOAD", str(ex))
	#			break
	#		else:
	#			time.sleep(time_out)
	#			time_out = time_out ** 2
	#			attempts += 1
	#			continue


	return {
		"headers": {
			"Content-Type": "application/json",
		},
		"statusCode": 200,
		"body": json.dumps(data)
	}