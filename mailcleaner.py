import sqlite3
import re
import zlib


# connecting to the database index.sqlite where cleaned up data will be stored.
conn = sqlite3.connect('I:\\Python\\mail spider\\index.sqlite')
cur = conn.cursor()

# Dropping the tables is they already exists such that each time this program is run, data is processed freshly
cur.execute('''DROP TABLE IF EXISTS Messages''')
cur.execute('''DROP TABLE IF EXISTS Senders''')
cur.execute('''DROP TABLE IF EXISTS Subjects''')
cur.execute('''DROP TABLE IF EXISTS Replies''')


# Creating the table Messages with id, guid, time, sender_id, subject_id, header, body as columns
cur.execute('''CREATE TABLE IF NOT EXISTS Messages 
            (id INTEGER PRIMARY KEY, guid INTEGER UNIQUE, time TEXT, 
            sender_id INTEGER, subject_id INTEGER, header BLOB, body BLOB)''')

# Creating the table Senders with id, sender as columns 
cur.execute('''CREATE TABLE IF NOT EXISTS Senders
            (id INTEGER PRIMARY KEY, sender TEXT UNIQUE)''')

# Creating the table Subject with id, subject as columns
cur.execute('''CREATE TABLE IF NOT EXISTS Subjects
            (id INTEGER PRIMARY KEY, subject TEXT UNIQUE)''')

# Creating the table Replies with from_id and to_id as columns
cur.execute('''CREATE TABLE IF NOT EXISTS Replies
            (from_id INTEGER, to_id INTEGER)''')


# Connecting to content.sqlite with different cursor in read only mode to avoid accidental alteration.
conn_1 = sqlite3.connect('file:I:\\Python\\mail spider\\content.sqlite?mode=ro', uri=True)
cur_1 = conn_1.cursor()


# reading data from content.sqlite to operate on
cur_1.execute('SELECT email, time, subject, header, body FROM Messages')

senders = dict() # {key="email" : value=id }
subjects = dict() # {key="subject" : value=id }
guids = dict() # {key="guid" : value=id }

count = 0


# Loop for extracting necessary data, cleaning the data, and storing in index.sqlite.
for message_row in cur_1:
    
    
    # Separating sender, subject, and guid from current row from Messages table in content.sqlite.
    sender = message_row[0]
    subject = message_row[2]
    
    # guid is Message ID provided in the header of the email.
    x = re.findall('\nMessage-ID: (<\\S+@\\S+>)\n',message_row[3])
    guid = x[0]
    
    
    # Counting and printing every 10th row.
    count += 1
    
    if count % 10 == 0 : print(message_row[1], sender, subject)
    
    
    # Getting the id's of sender, subject, and guid from their respective dictionaries, and if there is no value fount, then default=None is returned.
    sender_id = senders.get(sender,None)
    subject_id = subjects.get(subject,None)
    guid_id = guids.get(guid,None)
    
    
    # If sender_id is None, that is, there is no entry nether in senders dictionary nor in senders table in index.sqlite. so we insert the current sender into the table and get its id.
    # also fills the Senders table in index.sqlite.
    if sender_id is None:
        cur.execute('INSERT OR IGNORE INTO Senders (sender) VALUES (?)', (sender,))
        conn.commit() # So that the sender data is written into the table and id column is assigned.
        
        # Getting the id of sender to put it in the senders dictionary.
        cur.execute('SELECT id FROM Senders WHERE sender=? LIMIT 1', (sender,))
        
        try :
            row = cur.fetchone()
            
            if row is not None:
                sender_id = row[0]
                senders[sender] = sender_id
        
        except Exception as error:
            print(f'unable to retrieve sender ID, error : {error}')
            break
   
    
    # Same as sender_id. if subject_id is None, then the subject is inserted into table Subjects in index.sqlite and the its id is extracted.    
    # Also fills the Subjects table in index.sqlite.
    if subject_id is None:
        cur.execute('INSERT OR IGNORE INTO Subjects (subject) VALUES (?)', (subject,))
        conn.commit() # So that the subject data is written into the table and id column is assigned.
        
        # Getting the id of subject to put it into subjects dictionary.
        cur.execute('SELECT id FROM Subjects WHERE subject=? LIMIT 1', (subject,))
        try:
            row = cur.fetchone()
            subject_id = row[0]
            subjects[subject] = subject_id
        
        except Exception as error:
            print(f'unable to retrieve subject ID, error : {error}')
            break
  
    
    # Inserting guid, time, sender_id, subject_id, header, and body into Messages table of index.sqlite. header and body are compressed using zlib library.     
    # Also fills the Messages table in index.sqlite.
    cur.execute('''INSERT OR IGNORE INTO Messages (guid, time, sender_id, subject_id, header, body)
                VALUES (?, ?, ?, ?, ?, ?)''', (guid, message_row[1], sender_id, subject_id,
                                               zlib.compress(message_row[3].encode()), zlib.compress(message_row[4].encode())))
    
    conn.commit()
    
    # After inserting the current data into the table, now we can extract the its id to get the guid_id.
    cur.execute('SELECT id FROM Messages WHERE guid=? LIMIT 1', (guid,))       
    try: 
        row = cur.fetchone()
        guid_id = row[0]
        guids[guid] = guid_id
    
    except Exception as error:
            print(f'unable to retrieve ID for guid, error : {error}')
            break
  
        
# Reading all headers from Messages table in content.sqlite to extract the 'sent to' email.
send_to = None
cur_1.execute('SELECT header, email FROM Messages')

# Using loop to browse through all the headers and find 'sent to' emails using regular expression.
for msg_row in cur_1:
    
    x = re.findall('\nTo: .* <(\\S+@\\S+)>[,|\n]', msg_row[0])
    
    if len(x) > 0 : 
        send_to = x[0]
    
    else:
        x = re.findall('\nTo: <?(\\S+@\\S+)>?\n', msg_row[0])
        send_to = x[0]
    
    
    # Inserting the ids from senders dictionaries into Replies table in index.sqlite. 
    cur.execute('INSERT OR IGNORE INTO Replies (from_id, to_id) VALUES (?, ?)',
                (senders.get(msg_row[1],0), senders.get(send_to, 0)))
    
    conn.commit()
    
    print('completed Replies table.')


# Closing connection to content.sqlite and index.sqlite databases. 
cur_1.close()
cur.close()