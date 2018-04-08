from flask import Flask, render_template, request, redirect, url_for
import json
import os
from cassandra.cluster import Cluster
import datetime as dt
from dateutil.relativedelta import relativedelta

cluster = Cluster()
session = cluster.connect('twitter_db')

app = Flask(__name__)

keywords={
	'q1':'Enter "Author Name" to get tweets in the decreasing order of date-time posted: ',
	'q2':'Enter Keyword to find tweets containing the keyword: Ordered by popularity: ',
	'q3':'Enter a hashtag to search for tweets in the decreasing order of date-time posted: ',
	'q4':'Enter an author name to retrieve all tweets that mentions the author: decreasing order of date-time posted: ',
	'q5':'Enter date to extract tweets based on popularity: ',
	'q6':'Enter a location to retrieve tweets from: ',
	'q7':'Enter a date to retrieve top 20 popular hashtags over the last 7 days',
	'q8':'Enter a date to delete the tweets: ',
	'q1midsem':'Enter a hashtag to search for: ',
	'q2midsem':'Enter a date to search for: '
}


def q1(keyword):
	query = "SELECT * FROM q1 WHERE author='"+keyword+"';"
	return session.execute(query)

def q2(keyword):
	query = "SELECT * FROM q2 WHERE keywords='"+keyword+"';"
	return session.execute(query)

def q3(keyword):
	query = "SELECT * FROM q3 WHERE hashtags='"+keyword+"';"
	return session.execute(query)

def q4(keyword):
	query = "SELECT * FROM q4 WHERE mentions='"+keyword+"';"
	return session.execute(query)

def q5(keyword):
	query = "SELECT * FROM q5 WHERE date='"+keyword+"';"
	return session.execute(query)

def q6(keyword):
	query = "SELECT * FROM q6 WHERE location='"+keyword+"';"
	return session.execute(query)

def q7(keyword):
	query = "CREATE TABLE IF NOT EXISTS hashtag_counter(hashtags text PRIMARY KEY, count counter);"
	session.execute(query)
	query = "CREATE TABLE IF NOT EXISTS hashtag_count(dummy text, count int, hashtags text, PRIMARY KEY(dummy, count, hashtags)) WITH CLUSTERING ORDER BY (count DESC);"
	session.execute(query)
	session.execute("TRUNCATE hashtag_counter;")
	session.execute("TRUNCATE hashtag_count;")

	date = keyword.split('-')
	day_c = dt.date(day=int(date[2]), month=int(date[1]), year=int(date[0]))

	for i in range(7):
		if(i!=0):
			day_c = day_c - relativedelta(days=1)
		query = "SELECT date, hashtags FROM q7 WHERE date='"+str(day_c)+"';"
		resultset = session.execute(query)

		for element in resultset:
			query="UPDATE hashtag_counter SET count = count + 1 WHERE hashtags = '"+element.hashtags+"';"
			session.execute(query)

	query = "SELECT hashtags, count FROM hashtag_counter;"
	dataset = session.execute(query)

	prepared = session.prepare('INSERT INTO hashtag_count JSON ?')

	for element in dataset:
		entry={
			"hashtags":element.hashtags,
			"count":element.count,
			"dummy":'dummy'
		}
		session.execute(prepared, [json.dumps(entry, indent=4, sort_keys=True, default=str)])

	return session.execute("SELECT hashtags, count FROM hashtag_count WHERE dummy='dummy' LIMIT 20")


def q8(keyword):
	query = "SELECT COUNT(*) AS count FROM q8 WHERE date='"+keyword+"';"
	resultset = session.execute(query)
	query = "DELETE FROM q8 WHERE date='"+keyword+"';"
	session.execute(query)
	return resultset

def q1midsem(keyword):
	query = "CREATE TABLE IF NOT EXISTS location_counter(location text PRIMARY KEY, count counter);"
	session.execute(query)
	query = "CREATE TABLE IF NOT EXISTS location_count(hashtag text, count int, location text, PRIMARY KEY(hashtag, count, location)) WITH CLUSTERING ORDER BY (count DESC);"
	session.execute(query)
	session.execute("TRUNCATE location_counter;")
	session.execute("TRUNCATE location_count;")


	query="SELECT hashtags, location FROM q1midsem WHERE hashtags='"+keyword+"';"
	resultset = session.execute(query)

	for element in resultset:
			query="UPDATE location_counter SET count = count + 1 WHERE location = '"+element.location+"';"
			session.execute(query)

	query = "SELECT location, count FROM location_counter;"
	dataset = session.execute(query)

	prepared = session.prepare('INSERT INTO location_count JSON ?')

	for element in dataset:
		entry={
			"location":element.location,
			"count":element.count,
			"hashtag":keyword
		}
		session.execute(prepared, [json.dumps(entry, indent=4, sort_keys=True, default=str)])

	query="SELECT hashtag, location, count FROM location_count WHERE hashtag='"+keyword+"';"

	return session.execute(query)

def q2midsem(keyword):
	query = "CREATE TABLE IF NOT EXISTS pair_counter(mention text, hashtag text, count counter, PRIMARY KEY((mention, hashtag)));"
	session.execute(query)
	query = "CREATE TABLE IF NOT EXISTS pair_count(date text, count int, mention text, hashtag text, PRIMARY KEY(date, count, mention, hashtag)) WITH CLUSTERING ORDER BY (count DESC);"
	session.execute(query)
	session.execute("TRUNCATE pair_counter;")
	session.execute("TRUNCATE pair_count;")


	query="SELECT * FROM q2midsem WHERE date='"+keyword+"';"
	resultset = session.execute(query)
	print(keyword)
	for element in resultset:
		print(element)
		query="UPDATE pair_counter SET count = count + 1 WHERE mention = '"+element.mentions+"' AND hashtag='"+element.hashtags+"';"
		session.execute(query)

	query = "SELECT mention, hashtag, count FROM pair_counter;"
	dataset = session.execute(query)

	prepared = session.prepare('INSERT INTO pair_count JSON ?')

	for element in dataset:
		entry={
			"mention":element.mention,
			"hashtag":element.hashtag,
			"count":element.count,
			"date":keyword
		}
		session.execute(prepared, [json.dumps(entry, indent=4, sort_keys=True, default=str)])

	query="SELECT mention, hashtag, count, date FROM pair_count WHERE date='"+keyword+"';"

	return session.execute(query)


@app.route('/')
def index():
	return render_template('index.html', var="Hey")

@app.route('/process/<questionLink>', methods=['GET'])
def process(questionLink):
	return render_template('process.html', display_text=keywords[questionLink], q_number=questionLink)

@app.route('/result', methods=['POST'])
def result():
	keyword = request.form['keyword']
	question_number = request.form['question_number']
	if(question_number=='q1'):
		resultset = q1(keyword)
	elif(question_number=='q2'):
		resultset = q2(keyword)
	elif(question_number=='q3'):
		resultset = q3(keyword)
	elif(question_number=='q4'):
		resultset = q4(keyword)
	elif(question_number=='q5'):
		resultset = q5(keyword)
	elif(question_number=='q6'):
		resultset = q6(keyword)
	elif(question_number=='q7'):
		resultset = q7(keyword)
	elif(question_number=='q8'):
		resultset = q8(keyword)
	elif(question_number=='q1midsem'):
		resultset = q1midsem(keyword)
	else:
		resultset = q2midsem(keyword)
	return render_template('results.html', resultset=resultset, question_number=question_number)


if __name__ == '__main__':
	app.run(debug=True)