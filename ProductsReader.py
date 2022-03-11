import random
import json
import psycopg2
from objects import Product

with open("resources/database.conf") as f:
    settings = json.load(f)

conn = psycopg2.connect(
    host=settings['host'], database=settings['database'],
    user=settings['user'], password=settings['password'])
cur = conn.cursor()

cur.execute("SELECT * FROM products;")

products = []
for data in cur.fetchall():
    if data[10] is None:
        print("NONE ERROR")
        continue
    products.append(Product(
        data[0], data[1], data[2], data[3], data[4], data[5],
        data[6], data[7], data[8], data[9], data[10]
    ))


def average_price(p: list[Product]):
    total = 0
    for product in p:
        total += product.selling_price
    print(f"Average price = {round(total / len(p)) / 100}")


def largest_offset(p: list[Product], s: Product):
    best_offset = 0
    best_product = None
    for product in p:
        offset = abs(product.selling_price - s.selling_price)
        if offset > best_offset:
            best_offset = offset
            best_product = product
    print(f"Product with largest price offset = {best_product}")
    print(f"Price of found = {best_product.selling_price / 100}")
    print(f"Price of search = {s.selling_price / 100}")


print(f"Product amount = {len(products)}")
average_price(products)
largest_offset(products, random.choice(products))
