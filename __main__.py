# Standard library imports
import calendar
import dateutil.parser as parser
from datetime import datetime, timedelta, timezone
import re
import time

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
		raise
	

def translate_text(url, translate_apikey, language, text):

	if "en" in language or "unk" in language:
		return text
		
	language_mapping = {
		"ger": "DE",
	}
	
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
		print("*** " + env + " ERROR TRANSLATING TEXT", e)
		return ""


# @DEV: Takes a date string and converts it to central time stamp in miliseconds
# @PARAM: _date is a string in the form of: "Mon, 20 May 2019 18:00:56 +0000"
def get_UTC_time(_date):
	utc_in_miliseconds = calendar.timegm(parser.parse(_date).timetuple()) * 1000
	return utc_in_miliseconds


# @DEV: Uses the feedparser library to extract all article URLs from an XML feed and return as a list.
# @PARAM: _feed_list is a list of feeds to parse.
def parse_feed(_nlu_url,_nlu_api_key,_classify_id, _todays_date_pretty, _todays_date_struct, _already_ingested, _use_sql, translate_url, translate_apikey, _feed_list=[]):
	article_map = {}
	today = datetime.now()
	today_utc = today.replace(tzinfo = timezone.utc)
	today_utc_milli = int(today_utc.timestamp() * 1000)

	for feed in _feed_list:
		data = feedparser.parse(feed['feed_url'])
		for item in data.entries:
			yesterday = datetime.now() - timedelta(days = 1)
			yesterday_utc = yesterday.replace(tzinfo = timezone.utc)
			yesterday_utc_milli = int(yesterday_utc.timestamp() * 1000)
			tomorrow = datetime.now() + timedelta(days = 1)
			tomorrow_utc = tomorrow.replace(tzinfo = timezone.utc)
			tomorrow_utc_milli = int(tomorrow_utc.timestamp() * 1000)
			language = "unk"
			article_title = ""
			
			if hasattr(data["feed"], "language") and data["feed"]["language"] != "":
				language = data["feed"]["language"]
			
			if hasattr(item, 'title'):
				article_title = item.title		
			else:
				continue
				
			if _use_sql:
				# Ensure Publish date is within 24 hours (past or future) of now, otherwise skip
				if (hasattr(item, 'published') and (get_UTC_time(item.published) > yesterday_utc_milli and get_UTC_time(item.published) < tomorrow_utc_milli)) or not hasattr(item, 'published'):
					# Translate title
					article_title = translate_text(translate_url, translate_apikey, language, article_title)
					if article_title == "":
						continue
					
					# Skip already ingested articles
					skip = False
					print("*** " + env + " CHECKING FEED AGAINST ALREADY INGESTED: ", feed['feed_name'], " NUMBER:", len(_already_ingested[feed['feed_name']]))
					for ingested_article in _already_ingested[feed['feed_name']]:
						if strip_characters(article_title.lower()) == strip_characters(ingested_article['article_title'].lower()):
							#print("*** " + env + " SKIPPING ITEM VS DB: ", item.title, " VS ", ingested_article['article_title'])
							skip = True
							break
					if skip:
						print("*** " + env + " SKIPPING ARTICLE USING DB: ", article_title, " FEED:", feed['feed_url'])
						continue
				else:
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
				class_map = classify_text(_nlu_url, _nlu_api_key, _classify_id, article_title)
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
				
	return {"article_map" : article_map }

# @DEV: Filter the articles by their title. It should be passed a title and a Boolean to use a swear word filter or not. 
# It will return True or False (False if it should be filtered out, True if not).
def filter_by_title(title, swear_flag):
	filtered_regexes = [r"\bshop",r"\bsale",r"\bphoto",r"\bprice","weed","marijuana","cannabis"]
	for r in filtered_regexes:
		if re.search(r,title):
			return False

	if swear_flag:
		SWEAR_LIST = ["4r5e","5h1t","5hit","a55","anal","anus","ar5e","arrse","arse","ass","ass-fucker","asses","assfucker","assfukka","asshole","assholes","asswhole","a_s_s","b!tch","b00bs","b17ch","b1tch","ballbag","balls","ballsack","bastard","beastial","beastiality","bellend","bestial","bestiality","bi+ch","biatch","bitch","bitcher","bitchers","bitches","bitchin","bitching","bloody","blow job","blowjob","blowjobs","boiolas","bollock","bollok","boner","boob","boobs","booobs","boooobs","booooobs","booooooobs","breasts","buceta","bugger","bum","bunny fucker","butt","butthole","buttmuch","buttplug","c0ck","c0cksucker","carpet muncher","cawk","chink","cipa","cl1t","clit","clitoris","clits","cnut","cock","cock-sucker","cockface","cockhead","cockmunch","cockmuncher","cocks","cocksuck ","cocksucked ","cocksucker","cocksucking","cocksucks ","cocksuka","cocksukka","cok","cokmuncher","coksucka","coon","cox","crap","cum","cummer","cumming","cums","cumshot","cunilingus","cunillingus","cunnilingus","cunt","cuntlick ","cuntlicker ","cuntlicking ","cunts","cyalis","cyberfuc","cyberfuck ","cyberfucked ","cyberfucker","cyberfuckers","cyberfucking ","d1ck","damn","dick","dickhead","dildo","dildos","dink","dinks","dirsa","dlck","dog-fucker","doggin","dogging","donkeyribber","doosh","duche","dyke","ejaculate","ejaculated","ejaculates ","ejaculating ","ejaculatings","ejaculation","ejakulate","f u c k","f u c k e r","f4nny","fag","fagging","faggitt","faggot","faggs","fagot","fagots","fags","fanny","fannyflaps","fannyfucker","fanyy","fatass","fcuk","fcuker","fcuking","feck","fecker","felching","fellate","fellatio","fingerfuck ","fingerfucked ","fingerfucker ","fingerfuckers","fingerfucking ","fingerfucks ","fistfuck","fistfucked ","fistfucker ","fistfuckers ","fistfucking ","fistfuckings ","fistfucks ","flange","fook","fooker","fuck","fucka","fucked","fucker","fuckers","fuckhead","fuckheads","fuckin","fucking","fuckings","fuckingshitmotherfucker","fuckme ","fucks","fuckwhit","fuckwit","fudge packer","fudgepacker","fuk","fuker","fukker","fukkin","fuks","fukwhit","fukwit","fux","fux0r","f_u_c_k","gangbang","gangbanged ","gangbangs ","gaylord","gaysex","goatse","God","god-dam","god-damned","goddamn","goddamned","hardcoresex ","hell","heshe","hoar","hoare","hoer","homo","hore","horniest","horny","hotsex","jack-off ","jackoff","jap","jerk-off ","jism","jiz ","jizm ","jizz","kawk","knob","knobead","knobed","knobend","knobhead","knobjocky","knobjokey","kock","kondum","kondums","kum","kummer","kumming","kums","kunilingus","l3i+ch","l3itch","labia","lmfao","lust","lusting","m0f0","m0fo","m45terbate","ma5terb8","ma5terbate","masochist","master-bate","masterb8","masterbat*","masterbat3","masterbate","masterbation","masterbations","masturbate","mo-fo","mof0","mofo","mothafuck","mothafucka","mothafuckas","mothafuckaz","mothafucked ","mothafucker","mothafuckers","mothafuckin","mothafucking ","mothafuckings","mothafucks","mother fucker","motherfuck","motherfucked","motherfucker","motherfuckers","motherfuckin","motherfucking","motherfuckings","motherfuckka","motherfucks","muff","mutha","muthafecker","muthafuckker","muther","mutherfucker","n1gga","n1gger","nazi","nigg3r","nigg4h","nigga","niggah","niggas","niggaz","nigger","niggers ","nob","nob jokey","nobhead","nobjocky","nobjokey","numbnuts","nutsack","orgasim ","orgasims ","orgasm","orgasms ","p0rn","pawn","pecker","penis","penisfucker","phonesex","phuck","phuk","phuked","phuking","phukked","phukking","phuks","phuq","pigfucker","pimpis","piss","pissed","pisser","pissers","pisses ","pissflaps","pissin ","pissing","pissoff ","poop","porn","porno","pornography","pornos","prick","pricks","pron","pube","pusse","pussi","pussies","pussy","pussys ","rectum","retard","rimjaw","rimming","s hit","s.o.b.","sadist","schlong","screwing","scroat","scrote","scrotum","semen","sex","sh!+","sh!t","sh1t","shag","shagger","shaggin","shagging","shemale","shi+","shit","shitdick","shite","shited","shitey","shitfuck","shitfull","shithead","shiting","shitings","shits","shitted","shitter","shitters ","shitting","shittings","shitty ","skank","slut","sluts","smegma","smut","snatch","son-of-a-bitch","spac","spunk","s_h_i_t","t1tt1e5","t1tties","teets","teez","testical","testicle","tit","titfuck","tits","titt","tittie5","tittiefucker","titties","tittyfuck","tittywank","titwank","tosser","turd","tw4t","twat","twathead","twatty","twunt","twunter","v14gra","v1gra","vagina","viagra","vulva","w00se","wang","wank","wanker","wanky","whoar","whore","willies","willy","xrated","xxx"]
		if any([element in title.split('-') for element in SWEAR_LIST]):
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
			r = requests.get(url + 'v1/get-article-by-ingestdate-magazine', params=params)
			r.raise_for_status()
			ingested_articles[feed['feed_name']] = r.json()
		except Exception as ex:
			print("*** " + env + " ERROR USING SQL DB ***")
			return False, {}
	return True, ingested_articles
	
# @DEV: Strips string of all non-letter or number characters
# @RET: replaced string
def strip_characters(title):
	return re.sub(r'[^\w\d]','',title)


def main(_param_dictionary):
	global env
	env = _param_dictionary['env']
	
	# We will use today's date as the pub_date
	todays_date_struct = time.strptime(datetime.today().strftime('%a, %d %b %Y'),'%a, %d %b %Y')
	todays_date_pretty = time.strftime('%a, %d %b %Y', todays_date_struct)
	
	if _param_dictionary['sql_db_enabled']:
		use_sql, already_ingested = get_ingested_articles(_param_dictionary['feed_list'],_param_dictionary['sql_db_url'],_param_dictionary['sql_db_apikey'])
	else:
		use_sql = False
		already_ingested = {}
	#print("**** " + env + " **** ALREADY INGESTED ARTICLES", already_ingested)
	
	
	parsed_feed_map = parse_feed(
		_nlu_url = _param_dictionary['sentiment_url'],
		_nlu_api_key = _param_dictionary['sentiment_apikey'],
		_classify_id = _param_dictionary['nlc_id'],
		_feed_list=_param_dictionary['feed_list'],     
		_todays_date_struct = todays_date_struct,
		_todays_date_pretty = todays_date_pretty,
		_already_ingested = already_ingested,
		_use_sql = use_sql,
		translate_url = _param_dictionary["translate_url"],
		translate_apikey = _param_dictionary["translate_apikey"]
		)

	parsed_feed = parsed_feed_map['article_map']

	return {
	'discovery_version': _param_dictionary['discovery_version'],
	'discovery_url': _param_dictionary['discovery_url'],
	'discovery_api_key': _param_dictionary['discovery_api_key'],
	'collection_id': _param_dictionary['collection_id'],
	'environment_id': _param_dictionary['environment_id'],
	'sql_db_url':_param_dictionary['sql_db_url'],
	'sql_db_apikey': _param_dictionary['sql_db_apikey'],
	'sql_db_enabled': _param_dictionary['sql_db_enabled'],
	'sentiment_url': _param_dictionary['sentiment_url'],
	'sentiment_apikey': _param_dictionary['sentiment_apikey'],
	'sentiment_model': _param_dictionary['sentiment_model'],
	'translate_url': _param_dictionary["translate_url"],
	'translate_apikey': _param_dictionary["translate_apikey"],
	'parsed_feed': parsed_feed,
	'env': _param_dictionary["env"]
	}