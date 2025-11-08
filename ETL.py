import requests
import pandas as pd
import json
import random
import sqlite3
from pandas import json_normalize
from datetime import datetime, timedelta

## Tentukan range waktu untuk generate timestamp
start = datetime(2025, 10, 1)
end = datetime(2025, 10, 31, 23, 59, 59)

def initialLoad():
    conn = sqlite3.connect("Ecommerce.db")

    ## Create tabel di sqlite
    conn.execute("""
    CREATE TABLE IF NOT EXISTS sales (
        transaction_id INTEGER PRIMARY KEY,
        product_id INTEGER NOT NULL,
        product_name TEXT NOT NULL,
        category INTEGER,
        quantity INTEGER CHECK(quantity > 0), 
        price REAL NOT NULL CHECK(price > 0),
        total_sales REAL,
        transaction_date TEXT
    )       
    """)

    conn.close()

def extract():
    ## Get json dengan request
    r_product = requests.get('https://dummyjson.com/products?limit=200')
    r_carts = requests.get('https://dummyjson.com/carts?limit=50')

    ## Load data products
    df_product = json_normalize(r_product.json()['products'])

    ## Load data carts
    df_sales = pd.DataFrame()
    for item in r_carts.json()['carts']:
        random.seed(int(item['id']))
        df_temp = json_normalize(item['products'])
        df_temp['transaction_date'] = start + (end - start) * random.random() ##Random timestamp berdasarkan cartID
        df_temp['cartID'] = item['id']
        df_sales = pd.concat([df_sales,df_temp],ignore_index=True)  

    return df_product, df_sales

def transform(df_product, df_sales):
    ## Ekstrak kolom yang penting
    df_product = df_product[['id','title','description','category','price']]
    df_sales = df_sales[['id','title','price','quantity','transaction_date','cartID']]

    ## Normalisasi kategori produk
    df_product['category_id'] = df_product['category'].astype('category').cat.codes + 1

    ## Join data product dan sales untuk mendapatkan informasi penuh
    df_merge = pd.merge(df_product,df_sales,on='id',how='inner')
    df_merge = df_merge[['id','title_x','category','quantity','price_y','transaction_date','cartID','category_id']].rename(columns ={
            'id': 'product_id',
            'title_x': 'product_name',
            'price_y': 'price', 
        }
    )
    df_merge['total_sales'] = df_merge['quantity']*df_merge['price']
    df_merge.sort_values(by=['transaction_date','category_id'], ascending=[True,True], inplace=True)
    df_merge.insert(0,'transaction_id', range(1,len(df_merge)+1))

    ## Tambahkan transaction_id & restruktur data agar dapat dimasukkan ke database 
    df_final = df_merge[['transaction_id','product_id','product_name','category_id','quantity','price','total_sales','transaction_date']].rename(
        columns={'category_id':'category'})
    df_final.set_index('transaction_id',inplace=True)

    return df_final

def load(df_final):
    conn = sqlite3.connect("Ecommerce.db")
    conn.execute("DELETE FROM sales")

    ## Insert data ke database
    try:
        df_final.to_sql('sales',conn,if_exists='append')
    except Exception as e:
        print("Error: ", e)

    ## Output data ke file .sql
    with open("sales.sql", "w", encoding="utf-8") as f:
        for line in conn.iterdump():
            f.write(f"{line}\n")

    conn.close()

initialLoad()
prod, sales = extract()
final = transform(prod, sales)
load(final)

