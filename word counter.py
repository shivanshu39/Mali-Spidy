import sqlite3
import string




# Try and except the connection to the database index.sqlite.
try:
    conn = sqlite3.connect('file:I:\\Python\\mail spider\\index.sqlite?mode=ro', uri=True)
    cur = conn.cursor()
    
except Exception as err:
    print(f' unable to make connection : {err}')
    
    
    
    
# getting id, and subject from Subjects table in index.sqlite    
cur.execute('select id, subject from Subjects')

subjects = dict() # {key='id' : value='subject'}

for row in cur:
    subjects[row[0]] = row[1]




# getting subject_id from Messages table in index.sqlite    
cur.execute('select subject_id from Messages')


# To count each word in subjects.
counter = dict() # {key='word' : value=count}
text = ''

for row in cur:
    
    # Storing current subject to text.
    text = subjects[row[0]]
    
    # Cleaning the subject of any punctuations. 
    text = text.translate(str.maketrans('','', string.punctuation))
    
    # Cleaning the subject of any numbers 0-9.
    text = text.translate(str.maketrans('','','1234567890'))
    
    # Clearing white spaces from start and end of the subject and lowercasing the subject.
    text = text.strip().lower()
    
    # Splitting the subject(string of words) to list of each single word. 
    words = text.split()
    
    # Counting the each word with >4 letters and storing in the counter dictionary 
    for word in words:
        
        # only words with more than 4 letters will be counted.
        if len(word) < 4: 
            continue
        
        counter[word] = counter.get(word,0) + 1
        
        
        
        
# Sorting the counter by word count in descending order such that highest count is on top.        
x = sorted(counter, key=counter.get, reverse=True)

# User input to show asked amount of words.
limit = int(input('how many words:'))

for w in x[:limit]:
    print(f'word : {w} - count : {counter[w]}')




# Getting the highest and lowest of the top 100 words.
highest = None
lowest = None

for k in x[:100]:
    
    if highest is None or highest < counter[k]:
        highest = counter[k]
        
    if lowest is None or lowest > counter[k]:
        lowest = counter[k]
    
    
# To calculate the font size(20-100) to put in json file.       
bigsize = 80
smallsize = 20


# Create/open the words.js in write mode to write the data.

'''words = [{text:'word', size:size},
{text:'word', size:size},
...

];'''

with open('mail spider\\words.js', 'w') as fhandle:
    
    fhandle.write('words = [')
    
    first = True
    for k in x[:100]:
        
        if not first : 
            fhandle.write(',\n')
            
        first = False
        size = counter[k]
        size = (size - lowest) / float(highest - lowest)
        size = int((size*bigsize) + smallsize)
        
        fhandle.write('{' + "text: " + f"'{k}'" + ', size: '+ f'{size}' + '}') 
           
    fhandle.write('\n];\n')
    
    
    
    
# Closing the connection to database index.sqlite
cur.close()
