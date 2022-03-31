from mongodb import Mongo, Entry
from postgresql import Postgres

db = Mongo()
pg = Postgres()


indices = {}


def table_index(name, value):
    if type(value) is list:
        value = value[0]
    if value is None:
        return None
    if name not in indices:
        indices[name] = []
    idx = pg.index(name, 'name', value)
    if value not in indices[name]:
        indices[name].append(value)
        if idx == -1:
            pg.insert(name, name=value)
    return pg.index(name, 'name', value) if idx == -1 else idx


def safe_integer(val):
    try:
        val = str(val)
        if "." in val:
            flt = float(val) * 100.0
            return int(flt)
        return int(val)
    except ValueError:
        return 0


def product(entry: Entry):
    brand_id = table_index("brands", entry.var("brand"))
    gender_id = table_index("genders", entry.var("gender"))
    category_id = table_index("categories", entry.var("category"))
    sub_category_id = table_index("sub_categories", entry.var("sub_category"))
    sub_sub_category_id = table_index("sub_sub_categories", entry.var("sub_sub_category"))
    pg.insert("products",
              id=entry.var("_id"),
              fast_mover=entry.var("fast_mover"),
              herhaalaankopen=entry.var("herhaalaankopen"),
              name=entry.var("name"),
              predict_out_of_stock_date=entry.var("predict_out_of_stock_date"),
              price_discount=safe_integer(entry.var("price.discount")),
              price_mrsp=safe_integer(entry.var("price.mrsp")),
              selling_price=safe_integer(entry.var("price.selling_price")),
              brand_id=brand_id,
              gender_id=gender_id,
              category_id=category_id,
              sub_category_id=sub_category_id,
              sub_sub_category_id=sub_sub_category_id
              )


db.get_collection("products").for_each(product)
db.close()
pg.commit()
pg.close()
