# Mail spider
## This is a project made by following the tutorial from FreeCodeCamp.org - Python for Everyone.

In this project, extracting data from a URL, parsing it, and storing it in a database(SQLite), retrieving the data from the database, and doing operations on the data to store it into a JSON file are done.

The following URL was used to get data from. this URL is set up by the instructor of FreeCodeCamp.org - Python for Everyone.

URL - <a href = 'https://mbox.dr-chuck.net/sakai.devel/1/2'>https://mbox.dr-chuck.net/sakai.devel/1/2</a>

getmail.py uses the above-mentioned URL to extract data(emails). From the received data, by the use of regular expression, the sender's email, sent time, subject, header, and message body are extracted and stored in content.sqlite file.

mailcleaner.py then retrieve data from the content.sqlite file to separate the data into different tables and compress the header and body to reduce the size of the database and remove repeated entries to store the cleaned data in index.sqlite file

sender by month.py retrieves the required data from the index.sqlite file to calculate which organizations have been most active in each month, and store the calculated data into sender by month.js file.

top senders.py will show the requested number of top senders and top organizations.

word counter.py counts the repeated words in all of the subjects, and stores the words in word counter.js along the calculated size based on the word's count.
