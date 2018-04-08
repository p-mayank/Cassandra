import json
import os
from cassandra.cluster import Cluster
from cassandra.query import BatchStatement
from cassandra.query import SimpleStatement

cluster = Cluster()
session = cluster.connect('twitter_db')

def checkNullList(element):
	if element:
		if all('' == s or s.isspace() for s in element):
			return False
		else:
			return True
	return False

def checkLocation(element):
	if element:
		return True
	return False

def clean_db():
	session.execute("DROP TABLE IF EXISTS twitter;")
	session.execute("DROP TABLE IF EXISTS q1;")
	session.execute("DROP TABLE IF EXISTS q2;")
	session.execute("DROP TABLE IF EXISTS q3;")
	session.execute("DROP TABLE IF EXISTS q4;")
	session.execute("DROP TABLE IF EXISTS q5;")
	session.execute("DROP TABLE IF EXISTS q6;")
	session.execute("DROP TABLE IF EXISTS q7;")
	session.execute("DROP TABLE IF EXISTS q8;")

def create_tables():
	clean_db()
	query = "CREATE TABLE twitter (tid text PRIMARY KEY,author text,author_id text,author_profile_image text,author_screen_name text,date text,datetime timestamp,hashtags list<text>,keywords_processed_list list<text>,lang text,like_count int,location text,media_list map<text,frozen<map<text,text>>>,mentions list<text>,quote_count int,quoted_source_id text,reply_count int,replyto_source_id text,retweet_count int,retweet_source_id text,sentiment int,tweet_text text,type text,url_list list<text>,verified text);"
	session.execute(query)
	query = "CREATE TABLE IF NOT EXISTS q1 (tid text,author text,author_id text,datetime timestamp, lang text,location text, tweet_text text,PRIMARY KEY (author, datetime, tid)) WITH CLUSTERING ORDER BY (datetime DESC);"
	session.execute(query)
	query = "CREATE TABLE IF NOT EXISTS q2 (tid text,author text,author_id text,datetime timestamp, keywords text,lang text,like_count int,location text, tweet_text text, PRIMARY KEY (keywords, like_count, tid)) WITH CLUSTERING ORDER BY (like_count DESC);"
	session.execute(query)
	query = "CREATE TABLE IF NOT EXISTS q3 (tid text,author text,author_id text,datetime timestamp,lang text,hashtags text,location text,tweet_text text,PRIMARY KEY (hashtags, datetime, tid)) WITH CLUSTERING ORDER BY (datetime DESC);"
	session.execute(query)
	query = "CREATE TABLE IF NOT EXISTS q4 (tid text,author text,author_id text,datetime timestamp,lang text,location text,mentions text,tweet_text text, PRIMARY KEY (mentions, datetime, tid, author)) WITH CLUSTERING ORDER BY (datetime DESC);"
	session.execute(query)
	query = "CREATE TABLE IF NOT EXISTS q5 (tid text,author text,author_id text,date text,lang text,like_count int,location text,tweet_text text, PRIMARY KEY (date, like_count, tid)) WITH CLUSTERING ORDER BY (like_count DESC);"
	session.execute(query)
	query = "CREATE TABLE IF NOT EXISTS q6 (tid text,author text,author_id text,datetime timestamp,lang text,location text,tweet_text text, PRIMARY KEY (location, tid));"
	session.execute(query)
	query = "CREATE TABLE IF NOT EXISTS q7 (tid text,author text,author_id text,date text,hashtags text, lang text,location text,tweet_text text, PRIMARY KEY (date, hashtags, tid));"
	session.execute(query)
	query = "CREATE TABLE IF NOT EXISTS q8 (tid text,author text,author_id text,date text, lang text,location text,tweet_text text, PRIMARY KEY (date, tid));"
	session.execute(query)
	query="CREATE TABLE IF NOT EXISTS q1midsem (tid text, author text, location text, hashtags text, PRIMARY KEY(hashtags, location, tid));"
	session.execute(query)
	query="CREATE TABLE IF NOT EXISTS q2midsem (tid text, date text, mentions text, hashtags text, PRIMARY KEY(date, mentions, hashtags, tid));"
	session.execute(query)

# Default template: "SELECT * FROM q1 WHERE author='';"
def load_data_q1():
	prepared = session.prepare('INSERT INTO q1 JSON ?')
	query = "SELECT tid, author, author_id, datetime, lang, location, tweet_text FROM twitter;"
	out = session.execute(query)
	for ele in out:
		entry={
			"tid":ele.tid,
			"author":ele.author,
			"author_id":ele.author_id,
			"datetime":ele.datetime,
			"lang":ele.lang,
			"location":ele.location,
			"tweet_text":ele.tweet_text
		}
		print(entry)
		session.execute(prepared, [json.dumps(entry, indent=4, sort_keys=True, default=str)])

# Default template: "SELECT * FROM q2 WHERE keywords='friend';"
def load_data_q2():
	prepared = session.prepare('INSERT INTO q2 JSON ?')
	query = "SELECT tid, author, author_id, datetime, keywords_processed_list, lang, like_count, location, tweet_text FROM twitter;"
	out = session.execute(query)
	for ele in out:
		entry={
			"tid":ele.tid,
			"author":ele.author,
			"author_id":ele.author_id,
			"datetime":ele.datetime,
			"lang":ele.lang,
			"like_count":int(ele.like_count),
			"location":ele.location,
			"tweet_text":ele.tweet_text
		}
		print(entry)
		if(checkNullList(ele.keywords_processed_list)):
			for listelement in ele.keywords_processed_list:
				entry.update({"keywords":listelement})
				session.execute(prepared, [json.dumps(entry, indent=4, sort_keys=True, default=str)])

# Default template: "SELECT * FROM q3 WHERE hashtags='HoliHai';"
def load_data_q3():
	prepared = session.prepare('INSERT INTO q3 JSON ?')
	query = "SELECT tid, author, author_id, datetime, hashtags, lang, location, tweet_text FROM twitter;"
	out = session.execute(query)
	for ele in out:
		entry={
			"tid":ele.tid,
			"author":ele.author,
			"author_id":ele.author_id,
			"datetime":ele.datetime,
			"lang":ele.lang,
			"location":ele.location,
			"tweet_text":ele.tweet_text
		}
		if(checkNullList(ele.hashtags)):
			for listelement in ele.hashtags:
				entry.update({"hashtags":listelement})
				print(entry)
				session.execute(prepared, [json.dumps(entry, indent=4, sort_keys=True, default=str)])

#Default template : "SELECT * FROM q4 WHERE mentions='Lesbicando1995';"
def load_data_q4():
	prepared = session.prepare('INSERT INTO q4 JSON ?')
	query = "SELECT tid, author, author_id, datetime, mentions, lang, location, tweet_text FROM twitter;"
	out = session.execute(query)
	for ele in out:
		entry={
			"tid":ele.tid,
			"author":ele.author,
			"author_id":ele.author_id,
			"datetime":ele.datetime,
			"lang":ele.lang,
			"location":ele.location,
			"tweet_text":ele.tweet_text
		}
		if(checkNullList(ele.mentions)):
			for listelement in ele.mentions:
				entry.update({"mentions":listelement})
				print(entry)
				session.execute(prepared, [json.dumps(entry, indent=4, sort_keys=True, default=str)])

#Default template : "SELECT * FROM q5 WHERE date='';"
def load_data_q5():
	prepared = session.prepare('INSERT INTO q5 JSON ?')
	query = "SELECT tid, author, author_id, date, like_count, lang, location, tweet_text FROM twitter;"
	out = session.execute(query)
	for ele in out:
		entry={
			"tid":ele.tid,
			"author":ele.author,
			"author_id":ele.author_id,
			"like_count":int(ele.like_count),
			"date":ele.date,
			"lang":ele.lang,
			"location":ele.location,
			"tweet_text":ele.tweet_text
		}
		print(entry)
		session.execute(prepared, [json.dumps(entry, indent=4, sort_keys=True, default=str)])

#Default template : "SELECT * FROM q6 WHERE location='Southern Belle';"
def load_data_q6():
	prepared = session.prepare('INSERT INTO q6 JSON ?')
	query = "SELECT tid, author, author_id, datetime, lang, location, tweet_text FROM twitter;"
	out = session.execute(query)
	for ele in out:
		entry={
			"tid":ele.tid,
			"author":ele.author,
			"author_id":ele.author_id,
			"datetime":ele.datetime,
			"lang":ele.lang,
			"tweet_text":ele.tweet_text
		}
		if(checkLocation(ele.location)):
			entry.update({"location":ele.location})
			print(entry)
			session.execute(prepared, [json.dumps(entry, indent=4, sort_keys=True, default=str)])

#Default template : "SELECT * FROM q7 WHERE location='Southern Belle';"
def load_data_q7():
	prepared = session.prepare('INSERT INTO q7 JSON ?')
	query = "SELECT tid, author, author_id, hashtags, date, lang, location, tweet_text FROM twitter;"
	out = session.execute(query)
	for ele in out:
		entry={
			"tid":ele.tid,
			"author":ele.author,
			"author_id":ele.author_id,
			"date":ele.date,
			"lang":ele.lang,
			"location":ele.location,
			"tweet_text":ele.tweet_text
		}
		if(checkNullList(ele.hashtags)):
			for listelement in ele.hashtags:
				entry.update({"hashtags":listelement})
				print(entry)
				session.execute(prepared, [json.dumps(entry, indent=4, sort_keys=True, default=str)])

# Default template: "DELETE FROM q8 WHERE date='';"
def load_data_q8():
	prepared = session.prepare('INSERT INTO q8 JSON ?')
	query = "SELECT tid, author, author_id, date, lang, location, tweet_text FROM twitter;"
	out = session.execute(query)
	for ele in out:
		entry={
			"tid":ele.tid,
			"author":ele.author,
			"author_id":ele.author_id,
			"date":ele.date,
			"lang":ele.lang,
			"location":ele.location,
			"tweet_text":ele.tweet_text
		}
		print(entry)
		session.execute(prepared, [json.dumps(entry, indent=4, sort_keys=True, default=str)])

def load_data_q1midsem():
	prepared = session.prepare('INSERT INTO q1midsem JSON ?')
	query = "SELECT tid, author, location, hashtags FROM twitter;"
	out = session.execute(query)
	for ele in out:
		entry={
			"tid":ele.tid,
			"author":ele.author
		}
		if(checkLocation(ele.location)):
			entry.update({"location":ele.location})
			if(checkNullList(ele.hashtags)):
				for listelement in ele.hashtags:
					entry.update({"hashtags":listelement})
					print(entry)
					session.execute(prepared, [json.dumps(entry, indent=4, sort_keys=True, default=str)])

def load_data_q2midsem():
	prepared = session.prepare('INSERT INTO q2midsem JSON ?')
	query = "SELECT tid, date, mentions, hashtags FROM twitter;"
	out = session.execute(query)
	for ele in out:
		print(ele)
		entry={
			"tid":ele.tid,
			"date":ele.date
		}
		if(checkNullList(ele.hashtags)):
			for listelement in ele.hashtags:
				entry.update({"hashtags":listelement})
				if(checkNullList(ele.mentions)):
					for listelement in ele.mentions:
						entry.update({"mentions":listelement})
						print(entry)
						session.execute(prepared, [json.dumps(entry, indent=4, sort_keys=True, default=str)])


def main_table():
	prepared = session.prepare('INSERT INTO twitter JSON ?')
	all_files = os.listdir()
	for filename in all_files:
		filename_s = filename.split('.')
		if(filename_s[-1]=="json"):
			with open(filename) as json_data:
			    d_json = json.load(json_data)
			    for key, value in d_json.items():
			    	session.execute(prepared, [json.dumps(value)])

def main():
	create_tables()
	main_table()
	load_data_q1()
	load_data_q2()
	load_data_q3()
	load_data_q4()
	load_data_q5()
	load_data_q6()
	load_data_q7()
	load_data_q8()
	load_data_q1midsem()
	load_data_q2midsem()

if __name__ == '__main__':
	main()
