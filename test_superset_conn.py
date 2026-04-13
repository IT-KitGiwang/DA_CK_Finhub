from pyhive import hive
conn = hive.connect(host="master", port=10000, username="dack15", database="default", auth="NOSASL")
cursor = conn.cursor()
cursor.execute("SELECT * FROM crypto_trades LIMIT 3")
rows = cursor.fetchall()
for r in rows:
    print(r)
print("SUCCESS! Got", len(rows), "rows")
