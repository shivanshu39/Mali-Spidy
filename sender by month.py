import sqlite3




# Making connection to the database, index.sqlite in read only mode.
conn = sqlite3.connect('file:I:\\Python\\mail spider\\index.sqlite?mode=ro', uri=True)
cur = conn.cursor()




# Getting the message id, sender, sent time from join table of Messages and Senders.
cur.execute('select Messages.id, sender, time from Messages join Senders where Messages.sender_id = Senders.id')

messages = dict() # {key = 'message id' : value = (sender, time)}

# Storing the data into the messages dictionary while also slicing and striping the 'time' for suitable format(DD MMM YYYY).
for row in cur:
    messages[row[0]] = (row[1], row[2][2:11].strip())

# Closing the connection to database after getting all the required data.    
cur.close()




# Splitting the DNS from emails and storing the count of organization's mail into the dictionary sender_orgs.
sender_orgs = dict() # {key = 'org' : value = count}

for row_id, row in messages.items():
    sender = row[0]
    org = sender.split('@')
    
    if len(org) != 2 : 
        continue
    
    sender_orgs[org[1]] = sender_orgs.get(org[1], 0) + 1




# Sorting the sender_orgs dictionary by value to get the highest count on top and lowest at bottom.    
orgs = sorted(sender_orgs, key=sender_orgs.get, reverse=True)
orgs = orgs[:10] # storing only top 10 entries.
 
 
 
 
# Printing the top 10 organizations and their count.
print('Top 10 orgs are:')

for org in orgs:
    print(org, sender_orgs[org])




# Splitting the emails to get DNS and check with sender_orgs dict to verify, then add the months from time to months list.
count = dict() # {key=(month, 'org') : value = count}    
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
    
# sorting the list months by values in ascending order.    
months.sort()




# Creating and writing to the sender by months.js file.

'''senders = [ ['year','org1','org2','org3',..],
['dd mmm yyyy', count1, count2, count3,...], 
...
    
]; '''

with open('I:\\Python\\mail spider\\sender by month.js', 'w') as fhandle:
    fhandle.write("senders = [ ['Year', ")
    
    for org in orgs:
        fhandle.write(f"'{org}',")
    
    fhandle.write("],\n")
        
    for month in months:
        fhandle.write(f"['{month}'")
        
        for org in orgs:
            key = (month, org)
            fhandle.write(f",{str(count[key])}")
            
        fhandle.write("],\n")
    
    fhandle.write("\n];\n")
   
   
   
    
print("execution complete!")