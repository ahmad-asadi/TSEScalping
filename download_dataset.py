import datetime
import json
import time

import pytse_client as tse
import sqlite3 as sql
import requests


def dict_factory(cursor, row):
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d


def call_tse(date_str, symbol):
    retries = 0
    done = False
    transactions = {"tradeHistory": []}

    while not done and retries < 3:
        retries += 1

        url = "http://cdn.tsetmc.com/api/Trade/GetTradeHistory/%s/%s/true" % (symbol["ins_code"], date_str)
        print("retries:", retries, url)
        payload = {}
        headers = {
            'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:109.0) Gecko/20100101 Firefox/113.0',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate',
            'Origin': 'http://tsetmc.com',
            'Connection': 'keep-alive',
            'Referer': 'http://tsetmc.com/',
            'Upgrade-Insecure-Requests': '1',
        }

        api_response = requests.request("GET", url, headers=headers, data=payload)
        if api_response.status_code == 200:
            done = True
            transactions = json.loads(api_response.text)
        else:
            time.sleep(1)

    return transactions


conn = sql.connect("./Data.db")
conn.row_factory = dict_factory
cur = conn.cursor()

cur.execute('''
                CREATE TABLE IF NOT EXISTS Symbols
                         (id            INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
                         symbol         CHAR(50)        NOT NULL,
                         ins_code       INTEGER             NOT NULL);
         ''')

cur.execute('''
                CREATE TABLE IF NOT EXISTS Transactions
                         (id            INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
                         ins_code       INTEGER             NOT NULL,
                         DEven          INTEGER             NOT NULL,
                         HEven          INTEGER             NOT NULL,
                         quantity       INTEGER             NOT NULL,
                         price          INTEGER             NOT NULL,
                         foreign key (ins_code) references Symbols(ins_code)
                         );
         ''')

cur.execute('''
insert into Symbols (symbol, ins_code)
VALUES
('خگستر', 22811176775480091),
('دارا يكم', 62235397452612911) 
         ''')

conn.commit()

print("Tables created successfully")


def download_dataset(start_date_str="20210101", end_date_str="20230607"):
    current_date = datetime.datetime.strptime(start_date_str, "%Y%m%d")
    end_date = datetime.datetime.strptime(end_date_str, "%Y%m%d")
    results = cur.execute("SELECT * FROM Symbols").fetchall()
    for symbol in results:
        while current_date <= end_date:
            date_str = current_date.strftime("%Y%m%d")
            current_date += datetime.timedelta(days=1)

            transactions = call_tse(date_str, symbol)

            if len(transactions["tradeHistory"]) == 0:
                continue

            insert_query = """
            INSERT INTO Transactions (ins_code, DEven, HEven, quantity, price) VALUES 
            """
            for transaction in transactions["tradeHistory"]:
                insert_query += "(%d,%d,%d,%d,%d)," % (symbol["ins_code"], int(date_str), int(transaction["hEven"]),
                                                       int(transaction["qTitTran"]), int(transaction["pTran"]))

            cur.execute(insert_query[:-1])
            conn.commit()


if __name__ == '__main__':
    download_dataset()
