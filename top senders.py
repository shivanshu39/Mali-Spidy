import sqlite3




# User input for how many top senders and organizations they want to see.
how_many = int(input('How many Top senders and organizations to see: '))




# Connecting to database, index.sqlite in read only mode.
conn = sqlite3.connect('file:I:\\Python\\mail spider\\index.sqlite?mode=ro', uri=True)
cur = conn.cursor()




# Getting id and sender from Senders table in index.sqlite.
cur.execute('select id, sender from Senders')
senders = dict() # {key = 'id' : value = 'sender'}

for row in cur:
    senders[row[0]] = row[1]
  
  
    

# Getting message id, and sender from join of Messages and Sender tables.   
cur.execute('SELECT Messages.id, sender from Messages join Senders where Messages.sender_id = Senders.id')


mailcount = dict() # {key = 'email' : value = count}
org_mailcount = dict() # {key = 'dns' : value = count}

# Counting the senders email and dns to store in mailcount and org_mailcount dictionaries respectively.
for row in cur:
    sender_mail = row[1]
    mailcount[sender_mail] = mailcount.get(sender_mail, 0) + 1
    
    piece = sender_mail.split('@')

    if len(piece) != 2 : 
        continue
    
    org = piece[1]
    org_mailcount[org] = org_mailcount.get(org, 0) + 1




# Printing the asked amount of top senders.    
print(f'Top {how_many} emails!')
print('')

# Sorting mailcount by value in descending order such that highest count is on top.
x = sorted(mailcount, key=mailcount.get, reverse=True)
for k in x[:how_many]:
    print(f'{k} - Count : {mailcount[k]}')




# Printing the asked amount of top organizations.   
print(f'Top {how_many} domains!')
print('')

# Sorting mailcount by value in descending order such that highest count is on top.
x = sorted(org_mailcount, key=org_mailcount.get, reverse=True)
for k in x[:how_many]:
    print(f'{k} - Count : {org_mailcount[k]}')
    
    
    
    
# Closing the connection to the database, index.sqlite    
cur.close()