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

base_url = 'https://mbox.dr-chuck.net/sakai.devel/'

# connecting to the database
cunn = sqlite3.connect('I:\\Python\\mail spider\\content.sqlite')
cur = cunn.cursor()

cur.execute('''CREATE TABLE IF NOT EXISTS Messages 
            (id INTEGER UNIQUE, email TEXT, time TEXT, subject TEXT, header TEXT, body TEXT)''')

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
    
if start is None : start = 0

many = 0
count = 0
fail = 0
while True:
    if many < 1 :
        sval = input('How many message(s) to retrieve: ')
        if len(sval) < 1: break
        else:
            many = int(sval)
    
    start += 1
    many -= 1
    url = base_url + str(start) + '/' + str(start+1) 
    
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
        if fail > 2 : break
        continue # such that no following code gets executed and loop skips to next iteration.
    
    count += 1
    
    if not text.startswith('From '):
        print(f'"From " not found in url : {url}')
        fail += 1
        if fail > 2 : break
        continue # such that no following code gets executed and loop skips to next iteration.
    
    # separate header and body
    pos = text.find('\n\n')
    if pos > 0:
        header = text[:pos]
        body = text[pos+2:]
    else:
        print(f'Header not found in url : {url}')
        fail += 1
        if fail > 2 : break
        continue # such that no following code gets executed and loop skips to next iteration.
        
    email = ''
    x = re.findall('\nFrom: .* <(\\S+@\\S+)>\n', header)
    if len(x) == 1:
        email = x[0].strip().lower().replace('<','').replace('>','')        
    else:
        x = re.findall('\nFrom: (\\S+@\\S+)\n', header)
        if len(x) == 1:
            email = x[0].strip().lower().replace('<','').replace('>','') 
            
                        
    date = ''
    y = re.findall('\nDate: .*, (.*)\n', header)
    if len(y) == 1:
        tdate = y[0]
        date = tdate
        
        
    subject = ''
    z = re.findall('\nSubject: (.*)\n', header)
    if len(z) == 1 : 
        subject = z[0].strip().lower()
        
    fail = 0
    print(f'{email}, {date} - {subject}')
    cur.execute('INSERT OR IGNORE INTO Messages (id, email, time, subject, header, body) VALUES (?, ?, ?, ?, ?, ?)', (start, email, date, subject, header, body))
    if count % 50 == 0 : cunn.commit()
    if count % 100 == 0 : time.sleep(2)
cunn.commit()
cur.close()