# Cassandra
Extracting trends from twitter dataset downloaded using Twitter API, using cassandra DB.

* Please run the loadq file inside the dataset folder to create the initial tables. DB used is : 'twitter_db'

* Tables schema is detailed in the file: 'Schema.cql'

* Flask files include the 'templates' folder along with the executable 'flask_run.py' file

* External Libraries used are:
-'json' (Read/Write JSON files)
-'os' (Iterating over files)
-'cassandra' (Driver to connect to the cassandra db)
-'flask' (Setting up a web-interface.)
-'datetime' (Utility to parse date fields in Python)

// Recommended version of Python is 3.5;
