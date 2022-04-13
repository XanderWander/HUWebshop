from mongodb import Mongo
from postgresql import Postgres
from MongoToPG import read_products, read_sessions, read_visitors, inject
import time


class Databases:
    def __init__(self):
        self.db: Mongo
        self.pg: Postgres

    def __enter__(self):
        self.db = Mongo()
        self.pg = Postgres()
        return self.db, self.pg

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.db.close()
        self.pg.commit()
        self.pg.close()


def current_millis():
    return round(time.time() * 1000)


def drop_database():
    pg = Postgres()
    with open("resources/database.sql") as f:
        query = ''.join(f.readlines())
        pg.execute(query)
        pg.close()


def build_database():
    with Databases() as (db, pg):
        inject(db, pg)
        read_products()
        read_sessions()
        read_visitors()


def build_products():
    with Databases() as (db, pg):
        inject(db, pg)
        read_products()


def build_sessions():
    with Databases() as (db, pg):
        inject(db, pg)
        read_sessions()


def build_visitors():
    with Databases() as (db, pg):
        inject(db, pg)
        read_visitors()


def build_combined():
    with Databases() as (db, pg):
        with open("resources/others_combined.sql") as f:
            query = ''.join(f.readlines())
            pg.execute(query)


def order_count():
    with Databases() as (db, pg):
        with open("resources/order_count.sql") as f:
            query = ''.join(f.readlines())
            pg.execute(query)


def main():
    print("HUWebshop control interface")
    print("")
    print("0: Drop database (pg)")  # ~10s
    print("1: Build database (mongo>pg)")  # ~2100s > ~35m
    print("2: Build products only (mongo>pg)")  # ~75s
    print("3: Build sessions only (mongo>pg)")  # ~900s > ~15m
    print("4: Build visitors only (mongo>pg)")  # ~1100s > ~19m
    print("5: Create others combined table (pg)")  # ~15s
    print("6: Create order count table (pg)")  # ~3s
    print("")
    try:
        timing_start = current_millis()
        inp = int(input("Choose an option: "))
        match inp:
            case 0:
                drop_database()
            case 1:
                build_database()
            case 2:
                build_products()
            case 3:
                build_sessions()
            case 4:
                build_visitors()
            case 5:
                build_combined()
            case 6:
                order_count()
        time_ms = current_millis() - timing_start
        print("============================================")
        if time_ms > 1000:
            print(f"Last operation took {time_ms/1000}s")
        else:
            print(f"Last operation took {time_ms}ms")
        print("============================================")
    except ValueError:
        main()
    main()


if __name__ == "__main__":
    main()
