from mongodb import Mongo, Entry
from postgresql import Postgres


def product(entry: Entry):
    print(f"{entry.var('_id')}: {entry.var('price.selling_price')}")


db = Mongo()
db.get_collection("products").for_each(product, limit=10)

pg = Postgres()
pg.insert("table", id=0, name="example")

#
# INSERT INTO table(id, name) VALUES(0, 'example')
#
