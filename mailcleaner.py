import sqlite3
import re
import zlib

conn = sqlite3.connect('I:\\Python\\mail spider\\index.sqlite')
cur = conn.cursor()

cur.execute('''DROP TABLE IF EXISTS Messages''')
cur.execute('''DROP TABLE IF EXISTS Senders''')
cur.execute('''DROP TABLE IF EXISTS Subjects''')
cur.execute('''DROP TABLE IF EXISTS Replies''')

cur.execute('''CREATE TABLE IF NOT EXISTS Messages 
            (id INTEGER PRIMARY KEY, guid INTEGER UNIQUE, time TEXT, 
            sender_id INTEGER, subject_id INTEGER, header BLOB, body BLOB)''')

cur.execute('''CREATE TABLE IF NOT EXISTS Senders
            (id INTEGER PRIMARY KEY, sender TEXT UNIQUE)''')

cur.execute('''CREATE TABLE IF NOT EXISTS Subjects
            (id INTEGER PRIMARY KEY, subject TEXT UNIQUE)''')

cur.execute('''CREATE TABLE IF NOT EXISTS Replies
            (from_id INTEGER, to_id INTEGER)''')

conn_1 = sqlite3.connect('file:I:\\Python\\mail spider\\content.sqlite?mode=ro', uri=True)
cur_1 = conn_1.cursor()

cur_1.execute('SELECT email, time, subject, header, body FROM Messages')

senders = {}
subjects = {}
guids = {}

count = 0

for message_row in cur_1:
    
    sender = message_row[0]
    subject = message_row[2]
    
    x = re.findall('\nMessage-ID: (<\\S+@\\S+>)\n',message_row[3])
    guid = x[0]
    
    count += 1
    if count % 10 == 0 : print(message_row[1], sender, subject)
    
    sender_id = senders.get(sender,None)
    subject_id = subjects.get(subject,None)
    guid_id = guids.get(guid,None)
    
    if sender_id is None:
        cur.execute('INSERT OR IGNORE INTO Senders (sender) VALUES (?)', (sender,))
        conn.commit()
        cur.execute('SELECT id FROM Senders WHERE sender=? LIMIT 1', (sender,))
        try :
            row = cur.fetchone()
            if row is not None:
                sender_id = row[0]
                senders[sender] = sender_id
        except Exception as error:
            print(f'unable to retrieve sender ID, error : {error}')
            break
        
    if subject_id is None:
        cur.execute('INSERT OR IGNORE INTO Subjects (subject) VALUES (?)', (subject,))
        conn.commit()
        cur.execute('SELECT id FROM Subjects WHERE subject=? LIMIT 1', (subject,))
        try:
            row = cur.fetchone()
            subject_id = row[0]
            subjects[subject] = subject_id
        except Exception as error:
            print(f'unable to retrieve subject ID, error : {error}')
            break
        
    cur.execute('''INSERT OR IGNORE INTO Messages (guid, time, sender_id, subject_id, header, body)
                VALUES (?, ?, ?, ?, ?, ?)''', (guid, message_row[1], sender_id, subject_id,
                                               zlib.compress(message_row[3].encode()), zlib.compress(message_row[4].encode())))
    conn.commit()
    cur.execute('SELECT id FROM Messages WHERE guid=? LIMIT 1', (guid,))       
    try: 
        row = cur.fetchone()
        guid_id = row[0]
        guids[guid] = guid_id
    except Exception as error:
            print(f'unable to retrieve ID for guid, error : {error}')
            break
        
# sender_id to_id?
send_to = None
cur_1.execute('SELECT header, email FROM Messages')
for msg_row in cur_1:
    x = re.findall('\nTo: .* <(\\S+@\\S+)>[,|\n]', msg_row[0])
    if len(x) > 0 : 
        send_to = x[0]
    else:
        x = re.findall('\nTo: <?(\\S+@\\S+)>?\n', msg_row[0])
        send_to = x[0]
    
    cur.execute('INSERT OR IGNORE INTO Replies (from_id, to_id) VALUES (?, ?)',
                (senders.get(msg_row[1],0), senders.get(send_to, 0)))
    conn.commit()
    print('completed Replies table.')

cur_1.close()
cur.close()