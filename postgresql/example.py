import psycopg2

conn = psycopg2.connect(
                host="localhost",
                database="huwebshop",
                user="postgres",
                password="admin"
            )

cur = conn.cursor()

cur.execute(
    f"INSERT INTO products (deeplink, description, fast_mover, herhaalaankopen, name, predict_out_of_stock_date, price_discount, price_msrp, selling_price) "
    f"VALUES('','', False, False, '', current_date, 0, 0, 0);")
conn.commit()
cur.close()
conn.close()
