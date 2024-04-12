import json
import gzip
import os
import traceback

DATA_DIR = "/opt/spark/data/raw_data"


def parse(filename):
    f = gzip.open(filename, "r")
    entry = {}
    for l in f:
        l = l.strip().decode("utf-8")
        colonPos = l.find(":")
        if colonPos == -1:
            yield entry
            entry = {}
            continue
        eName = l[:colonPos]
        rest = l[colonPos + 2 :]
        entry[eName] = rest
    yield entry


list_files = os.listdir(DATA_DIR)

import mysql.connector

con = mysql.connector.connect(
    user="root", password="root", host="mysql-server", database="product_analysis"
)

curs = con.cursor(dictionary=True)

INSERT_PRODUCT_QUERY = (
    "INSERT ignore INTO products (id, title, processed, category) VALUES (%s,%s,%s,%s)"
)

count = 0
for file in list_files:
    try:
        for e in parse(os.path.join(DATA_DIR, file)):
            if not "product/productId" in e:
                continue

            product_id = e["product/productId"]
            product_title = e["product/title"]
            product_cateory = file.split(".")[0]
            review_text = e["review/summary"] + ' ' + e['review/text']
            review_rating = e["review/score"]
            helpfulness_split = e["review/helpfulness"].split("/")
            review_helpfulness = 0
            try:
                review_helpfulness = int(helpfulness_split[0]) / int(helpfulness_split[1])
            except:
                pass
            curs.execute(
                INSERT_PRODUCT_QUERY, (product_id, product_title, 0, product_cateory)
            )
            INSERT_REVIEW_QUERY = """INSERT INTO reviews (product_id, helpfulness, rating, summary) VALUES (%s, %s, %s, %s)"""
            curs.execute(
                INSERT_REVIEW_QUERY,
                (product_id, review_helpfulness, review_rating, review_text[:511]),
            )
            count += 1
            if count % 1000 == 0:
                con.commit()
                print(f"Inserted {count} rows.")
    except Exception as e:
        print(e)
        print(traceback.format_exception(e))
        continue
con.commit()
curs.close()
con.close()