from mongodb import Mongo, Entry
from postgresql import Postgres
from typing import Optional

db: Optional[Mongo] = None
pg: Optional[Postgres] = None
indices = {}


def table_index(name, value):
    """
    Get the row index of a value in Postgresql
    :param name: The column name
    :param value: The column value to search
    :return: The row index or -1 if not found
    """

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
    """
    Used for converting MongoDB data to an integer and auto-converts floats to integers
    :param val: The MongoDB integer or float value as string
    :return: The python safe integer value
    """

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
    """

    brand_id = table_index("brands", entry.var("brand"))
    gender_id = table_index("genders", entry.var("gender"))
    category_id = table_index("categories", entry.var("category"))
    sub_category_id = table_index("sub_categories", entry.var("sub_category"))
    sub_sub_category_id = table_index("sub_sub_categories", entry.var("sub_sub_category"))

    """
    Insert a new row in the main products table
    """
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

    """
    Insert all not-none properties of this product to the properties table
    """
    properties = entry.var("properties")
    if properties is not None:
        for name, value in properties.items():
            if value is None:
                continue
            pg.insert("properties",
                      name=name,
                      value=value,
                      product_id=entry.var("_id"))


def build_brand_preference(sid, entry: Entry):
    """
    Add a new brand preference of a session
    :param sid: The session id
    :param entry: The row data
    """

    preferences = entry.var(f"preferences.brand")
    if preferences is not None:
        for preference in preferences:
            idx = pg.index(f"brands", "name", preference)
            if idx != -1:
                pg.insert(f"brand_preference",
                          brand_id=idx,
                          session_id=sid,
                          views=entry.var(f"preferences.brand.{preference}.views"),
                          orders=entry.var(f"preferences.brand.{preference}.orders"))


def build_category_preference(sid, entry: Entry):
    """
    Add a new category preference of a session
    :param sid: The session id
    :param entry: The row data
    """

    preferences = entry.var(f"preferences.category")
    if preferences is not None:
        for preference in preferences:
            idx = pg.index(f"categories", "name", preference)
            if idx != -1:
                pg.insert(f"category_preference",
                          category_id=idx,
                          session_id=sid,
                          views=entry.var(f"preferences.category.{preference}.views"),
                          orders=entry.var(f"preferences.category.{preference}.orders"))


def session(entry: Entry):
    """
    Add a new session entry to the database
    :param entry: The entry to add
    """

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


    """
    Add all ordered products of this session to a separate table
    """
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

    """
    Add all preferences of this session to a separate table
    """
    build_brand_preference(sid, entry)
    build_category_preference(sid, entry)


def visitor(entry: Entry):
    """
    Add a new visitor (profile) entry to the database
    :param entry: The entry to add
    """

    pid = str(entry.var("_id"))
    pg.insert("profiles",
              id=pid,
              segment=entry.var("recommendations.segment"),
              has_email=entry.var("meta.has_email"))

    """
    Add all viewed before products of this profile to a separate table
    """
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

    """
    Add all buids of this profile to a separate table for session and profile linking
    """
    buids = entry.var("buids")
    if buids is not None:
        for buid in buids:
            if type(buid) is not dict and type(buid) is not list:
                pg.insert("buids",
                          buid=buid,
                          profile_id=pid
                          )


def read_products():
    """
    Read all products from MongoDB and call the product function for each row
    """

    db.get_collection("products").for_each(product)


def read_sessions():
    """
    Read all sessions from MongoDB and call the session function for each row
    """

    db.get_collection("sessions").for_each(session)


def read_visitors():
    """
    Read all visitors from MongoDB and call the visitor function for each row
    """

    db.get_collection("visitors").for_each(visitor)


def inject(mongo, postgres):
    """
    Inject the database handles from an external program to be used
    :param mongo: The external MongoDB handle
    :param postgres: The external Postgresql handle
    """

    global db, pg
    db = mongo
    pg = postgres
