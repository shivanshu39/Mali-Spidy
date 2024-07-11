import sqlite3

# make connection to the database
conn = sqlite3.connect('file:I:\\Python\\mail spider\\index.sqlite?mode=ro', uri=True)
cur = conn.cursor()

# extracting data from database
# orgs = {}

# cur.execute('select id, sender from Senders')
# for row in cur:
#     orgs[row[0]] = row[1]

cur.execute('select Messages.id, sender, time from Messages join Senders where Messages.sender_id = Senders.id')
messages = {}
for row in cur:
    messages[row[0]] = (row[1], row[2][2:11].strip())
    
cur.close()

sender_orgs = {}
for row_id, row in messages.items():
    sender = row[0]
    org = sender.split('@')
    
    if len(org) != 2 : 
        continue
    
    sender_orgs[org[1]] = sender_orgs.get(org[1], 0) + 1
    
orgs = sorted(sender_orgs, key=sender_orgs.get, reverse=True)
orgs = orgs[:10]
    
print('Top 10 orgs are:')
for org in orgs:
    print(org, sender_orgs[org])


count = {}    
months = []    
for row_id, row in messages.items():
    sender = row[0]
    dns = sender.split('@')
    
    if len(dns) != 2 : 
        continue
    
    if dns[1] not in orgs: 
        continue
    
    if row[1] not in months : 
        months.append(row[1])
    
    key = (row[1], dns[1])
    count[key] = count.get(key, 0) + 1
    
months.sort()

with open('I:\\Python\\mail spider\\sender by month.js', 'w') as fhandle:
    fhandle.write("senders = [ ['Year', ")
    
    for org in orgs:
        fhandle.write(f"'{org}' ,")
    
    fhandle.write("],\n")
        
    for month in months:
        fhandle.write(f"['{month}'")
        
        for org in orgs:
            key = (month, org)
            fhandle.write(f",{str(count[key])}")
            
        fhandle.write("],\n")
    
    fhandle.write("\n];\n")
    
print("execution complete!")