from typing import Optional
from pymongo import MongoClient
import psycopg2
import json

client = MongoClient("mongodb://localhost:27017/")
db = client.huwebshop

with open("resources/database.conf") as f:
    settings = json.load(f)

conn = psycopg2.connect(
    host=settings['host'], database=settings['database'],
    user=settings['user'], password=settings['password'])
cur = conn.cursor()

ids = []
brands = []
categories = []
colors = []
genders = []


def brand_index(brand) -> Optional[int]:
    if brand is None:
        return None
    if brand not in brands:
        brands.append(brand)
        cur.execute("INSERT INTO brands(name) VALUES(%s)", (brand,))
        conn.commit()
    return brands.index(brand) + 1


def category_index(category) -> Optional[int]:
    if category is None:
        return None
    if category not in categories:
        categories.append(category)
        cur.execute("INSERT INTO categories(name) VALUES(%s)", (category,))
        conn.commit()
    return categories.index(category) + 1


def color_index(color) -> Optional[int]:
    if color is None:
        return None
    if color not in colors:
        colors.append(color)
        cur.execute("INSERT INTO colors(name) VALUES(%s)", (color,))
        conn.commit()
    return colors.index(color) + 1


def gender_index(gender) -> Optional[int]:
    if gender is None:
        return None
    if gender not in genders:
        genders.append(gender)
        cur.execute("INSERT INTO genders(name) VALUES(%s)", (gender,))
        conn.commit()
    return genders.index(gender) + 1


def add_product(p) -> Optional[Exception]:
    query = "INSERT INTO products(" \
            "_id, deeplink, description, fast_mover, herhaalaankopen, name, predict_out_of_stock_date, price_discount, price_mrsp, selling_price, brand_id, category_id, color_id, gender_id) " \
            "VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"

    try:
        if cur is None or conn is None or conn.closed != 0:
            raise ConnectionError("Connection is already closed")
        price = p.get('price')
        if price is None:
            return ValueError("Price is None")
        if price.get('selling_price') is None:
            return ValueError("Selling price is None")
        cur.execute(query, (p['_id'], p.get('deeplink'), p.get('description'), p.get('fast_mover'),
                            p.get('herhaalaankopen'), p.get('name'), p.get('predict_out_of_stock_date'), price.get('discount'),
                            price.get('mrsp'), price.get('selling_price'),
                            brand_index(p.get('brand')), category_index(p.get('category')),
                            color_index(p.get('color')), gender_index(p.get('gender'))))

    except Exception as e:
        if conn is not None and conn.closed != 0:
            conn.rollback()
            conn.close()
        if cur is not None:
            cur.close()
        return e
    return None


AMOUNT_OF_PRODUCT_TO_MOVE = -1
counter = 0
failed = 0

for idx, product in enumerate(db.products.find()):
    error = add_product(product)
    if error:
        failed += 1
        print(f"{type(error)}: {error}")
        if conn.closed != 0 or cur.closed:
            print("An error occurred while adding a product")
            break
    else:
        counter += 1
    if idx == AMOUNT_OF_PRODUCT_TO_MOVE:
        conn.commit()
        cur.close()
        conn.close()
        break

print(f"{counter=}")
print(f"{failed=}")
