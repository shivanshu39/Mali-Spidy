import sqlite3
import time
import urllib
import re
import ssl
import urllib.request




# to ignore the certificate issue
ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE




#base url in which the required arguments will concatenated to retrieve the required data.
base_url = 'https://mbox.dr-chuck.net/sakai.devel/'




# connecting to the database. which will create the file if not exists.
cunn = sqlite3.connect('I:\\Python\\mail spider\\content.sqlite')
cur = cunn.cursor()




#creating the table Messages with id, email, time, subject, header, body columns.
cur.execute('''CREATE TABLE IF NOT EXISTS Messages 
            (id INTEGER UNIQUE, email TEXT, time TEXT, subject TEXT, header TEXT, body TEXT)''')




# to get the start point from where the last entry was made in table, if the last entry exists, then start will hold it's id. If no entry then start is O.
start = None
cur.execute('SELECT max(id) FROM Messages')

try: 
    row = cur.fetchone()
    
    if row is not None:
        start = row[0]
        
    else:
        start = 0

except:
    start = 0
    
if start is None : 
    start = 0




# After the start point is settled, extracting data from the url will be done. 
many = 0
count = 0
fail = 0
while True:
    
    # Asking the user to set the amount of emails to retrieve from url. 'many' variable is used to keep count of emails to retrieve and stop the while loop.
    if many < 1 :
        sval = input('How many message(s) to retrieve: ')
        
        if len(sval) < 1: 
            break
        
        else:
            many = int(sval)
    
    
    
    
    # setting up the URL with the arguments to retrieve data. also reducing the 'many' variable.
    start += 1
    many -= 1
    
    # the url requires two arguments, 'start/end'. if 1/2 is passed, the url will send first 1 email. if 1/3 then first 2 emails, and so on.
    url = base_url + str(start) + '/' + str(start+1) 
    
    
    
    
    # request to open the urk and retrieve the data inside of try and except block. if tyr block is successful, then retrieved data is stored in 'text' variable.
    text = 'None'
    
    try:
        document = urllib.request.urlopen(url, None, 30, context=ctx)
        text = document.read().decode()
        
        if document.getcode() != 200 :
            print(f'Error code : {document.getcode()} received.')
            break
    
    except KeyboardInterrupt:
        print('Interrupted by the user')
        break
    
    except Exception as error:
        print(f'Failed to retrieve or parse the following url \n{url}')
        print(f'Error : {error}')
        fail += 1
        
        if fail > 2 : 
            break
        
        continue # such that no following code gets executed and loop skips to next iteration.
    
    
    
    
    # if data is retrieved from the url, count is raised by one.
    count += 1
    
    
    
    
    # to check if the correct data is retrieved or not.
    if not text.startswith('From '):
        print(f'"From " not found in url : {url}')
        fail += 1
        
        if fail > 2 : 
            break
        
        continue # such that no following code gets executed and loop skips to next iteration.
    
    
    
    
    # separating header and body from the 'text' using the regular expression.
    pos = text.find('\n\n')
    
    if pos > 0:
        header = text[:pos]
        body = text[pos+2:]
    
    else:
        print(f'Header not found in url : {url}')
        fail += 1
        
        if fail > 2 : 
            break
        
        continue # such that no following code gets executed and loop skips to next iteration.
    
    
    
    
    # extracting email id from the header using regular expression.    
    email = ''
    x = re.findall('\nFrom: .* <(\\S+@\\S+)>\n', header)
    
    if len(x) == 1:
        email = x[0].strip().lower().replace('<','').replace('>','')        
    
    else:
        x = re.findall('\nFrom: (\\S+@\\S+)\n', header)
        
        if len(x) == 1:
            email = x[0].strip().lower().replace('<','').replace('>','') 
    
    
    
    
            
    # extracting sent time from the header using regular expression.                    
    date = ''
    y = re.findall('\nDate: .*, (.*)\n', header)
    
    if len(y) == 1:
        tdate = y[0]
        date = tdate
     
     
     
             
    # extracting the subject from the header using regular expression.    
    subject = ''
    z = re.findall('\nSubject: (.*)\n', header)
    
    if len(z) == 1 : 
        subject = z[0].strip().lower()
    
    
    
    
    # resetting the fail counter and inserting the extracted data to the table Messages.     
    fail = 0
    print(f'{email}, {date} - {subject}')
    cur.execute('INSERT OR IGNORE INTO Messages (id, email, time, subject, header, body) VALUES (?, ?, ?, ?, ?, ?)', (start, email, date, subject, header, body))
    
    
    
    
    if count % 50 == 0 : 
        cunn.commit() # making the data to be written on the content.sqlite file as only insert query does not write the data to the file directly.
    
    if count % 100 == 0 : 
        time.sleep(2) # a 2 second sleep so not to overburden the server or to stop the program if needed.
 
 
 
    
# final commit and closing the connection to the database(content.sqlite)    
cunn.commit()
cur.close()