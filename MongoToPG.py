from mongodb import Mongo, Entry
from postgresql import Postgres
from typing import Optional

db: Optional[Mongo] = None
pg: Optional[Postgres] = None

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
    """
    Add a new product entry to the database
    :param entry: The entry to add
    :return: Nothing
    """

    brand_id = table_index("brands", entry.var("brand"))
    gender_id = table_index("genders", entry.var("gender"))
    category_id = table_index("categories", entry.var("category"))
    sub_category_id = table_index("sub_categories", entry.var("sub_category"))
    sub_sub_category_id = table_index("sub_sub_categories", entry.var("sub_sub_category"))

    # Main products table
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

    # Properties table
    properties = entry.var("properties")
    if properties is not None:
        for name, value in properties.items():
            if value is None:
                continue
            pg.insert("properties",
                      name=name,
                      value=value,
                      product_id=entry.var("_id"))


def build_preference(name, name_s, sid, entry: Entry):
    preferences = entry.var(f"preferences.{name}")
    if preferences is not None:
        for preference in preferences:
            idx = pg.index(f"{name_s}", "name", preference)
            if idx != -1:
                if name == "brand":
                    pg.insert(f"{name}_preference",
                              brand_id=idx,
                              session_id=sid,
                              views=entry.var(f"preferences.{name}.{preference}.views"),
                              orders=entry.var(f"preferences.{name}.{preference}.orders"))
                else:
                    pg.insert(f"{name}_preference",
                              category_id=idx,
                              session_id=sid,
                              views=entry.var(f"preferences.{name}.{preference}.views"),
                              orders=entry.var(f"preferences.{name}.{preference}.orders"))


def session(entry: Entry):

    # Sessions
    buids = entry.var("buid")
    buid = None
    if buids is not None:
        if len(buids) > 0:
            buid = buids[0]
    sid = str(entry.var("_id"))
    pg.insert("sessions",
              id=sid,
              buid=buid,
              os=entry.var("user_agent.os.familiy"),
              has_sale=entry.var("has_sale"))

    # Ordered products
    ordered = entry.var("order.products")
    if ordered is not None:
        for order in ordered:
            oid = order.get('id')
            if id is not None:
                idx = pg.index("products", "id", oid)
                if idx != -1:
                    pg.insert("ordered_products",
                              product_id=oid,
                              session_id=sid
                              )

    # Preferences
    build_preference("brand", "brands", sid, entry)
    build_preference("category", "categories", sid, entry)


def visitor(entry: Entry):

    pid = str(entry.var("_id"))
    pg.insert("profiles",
              id=pid,
              segment=entry.var("recommendations.segment"),
              has_email=entry.var("meta.has_email"))

    # Viewed products
    viewed = entry.var("recommendations.viewed_before")
    if viewed is not None:
        for prod in viewed:
            if type(prod) is not dict and type(prod) is not list:
                idx = pg.index("products", "id", prod)
                if idx != -1:
                    pg.insert("viewed_before",
                              product_id=prod,
                              profile_id=pid
                              )

    # Sessions
    buids = entry.var("buids")
    if buids is not None:
        for buid in buids:
            if type(buid) is not dict and type(buid) is not list:
                pg.insert("buids",
                          buid=buid,
                          profile_id=pid
                          )


def read_products():
    db.get_collection("products").for_each(product)


def read_sessions():
    db.get_collection("sessions").for_each(session)


def read_visitors():
    db.get_collection("visitors").for_each(visitor)


def inject(mongo, postgres):
    global db, pg
    db = mongo
    pg = postgres
