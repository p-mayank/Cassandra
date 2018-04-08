use twitter_db;

CREATE TABLE twitter(
	tid text,
	quote_count int,
	reply_count int, 
	hashtags list<text>,
	datetime timestamp,
	date text,
	keywords_processed_list list<text>,
	like_count int,
	mentions list<text>,
	replyto_source_id text,
	verified Boolean,
	sentiment int,
	author text,
	location text,
	retweet_count int,
	type text,
	media_list map<text,frozen<map<text, text>>>,
	quoted_source_id text,
	url_list list<text>,
	tweet_text text,
	author_profile_image text,
	author_screen_name text,
	author_id text,
	lang text,
	PRIMARY KEY(tid)
)