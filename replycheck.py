import sqlite3
import re

conn = sqlite3.connect('file:I:\\Python\\mail spider\\content.sqlite?mode=ro', uri=True)
cur = conn.cursor()

cur.execute('SELECT header FROM Messages LIMIT 10')

for msg_row in cur:
    x = re.findall('\nTo: .* <(\\S+@\\S+)>[,|\n]', msg_row[0])
    if len(x) > 0 : 
        print(x)
    else:
        x = re.findall('\nTo: <?(\\S+@\\S+)>?\n', msg_row[0])
        print(x)