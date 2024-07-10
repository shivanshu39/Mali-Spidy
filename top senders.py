import sqlite3

how_many = int(input('How many Top senders and organizations to see: '))

conn = sqlite3.connect('file:I:\\Python\\mail spider\\index.sqlite?mode=ro', uri=True)
cur = conn.cursor()

cur.execute('select id, sender from Senders')
senders ={}
for row in cur:
    senders[row[0]] = row[1]
    
    
cur.execute('SELECT Messages.id, sender from Messages join Senders where Messages.sender_id = Senders.id')

mailcount = {}
org_mailcount = {}
message_count = 0

for row in cur:
    sender_mail = row[1]
    mailcount[sender_mail] = mailcount.get(sender_mail, 0) + 1
    piece = sender_mail.split('@')
    if len(piece) != 2 : continue
    org = piece[1]
    org_mailcount[org] = org_mailcount.get(org, 0) + 1
    message_count += 1
    
print(f'Top {how_many} emails!')
print('')

x = sorted(mailcount, key=mailcount.get, reverse=True)
for k in x[:how_many]:
    print(f'{k} - Count : {mailcount[k]}')

print(f'Top {how_many} domains!')
print('')

x = sorted(org_mailcount, key=org_mailcount.get, reverse=True)
for k in x[:how_many]:
    print(f'{k} - Count : {org_mailcount[k]}')
    
cur.close()