import sqlite3
import string
try:
    conn = sqlite3.connect('file:I:\\Python\\mail spider\\index.sqlite?mode=ro', uri=True)
    cur = conn.cursor()
except Exception as err:
    print(f' unable to make connection : {err}')
    
cur.execute('select id, subject from Subjects')
subjects = {}

for row in cur:
    subjects[row[0]] = row[1]
    
cur.execute('select subject_id from Messages')

counter = {}
text = ''
for row in cur:
    text = subjects[row[0]]
    text = text.translate(str.maketrans('','', string.punctuation))
    text = text.translate(str.maketrans('','','1234567890'))
    text = text.strip().lower()
    words = text.split()
    for word in words:
        if len(word) < 4: continue
        counter[word] = counter.get(word,0) + 1
        
x = sorted(counter, key=counter.get, reverse=True)
limit = int(input('how many words:'))
for w in x[:limit]:
    print(f'word : {w} - count : {counter[w]}')

highest = None
lowest = None
for k in x[:100]:
    if highest is None or highest < counter[k]:
        highest = counter[k]
        
    if lowest is None or lowest > counter[k]:
        lowest = counter[k]
        
bigsize = 80
smallsize = 20

with open('mail spider\\words.js', 'w') as fhandle:
    fhandle.write('words = [')
    first = True
    for k in x[:100]:
        if not first : fhandle.write(',\n')
        first = False
        size = counter[k]
        size = (size - lowest) / float(highest - lowest)
        size = int((size*bigsize) + smallsize)
        fhandle.write('{' + "text: " + f"'{k}'" + ', size: '+ f'{size}' + '}')    
    fhandle.write('\n];\n')
cur.close()
