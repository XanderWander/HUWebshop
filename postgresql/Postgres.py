import psycopg2


class Postgres:
    def __init__(self,
                 host="localhost",
                 database="huwebshop",
                 user="postgres",
                 password="admin"):
        self.host = host
        self.database = database
        self.user = user
        self.password = password
        self.query = "INSERT INTO %s(%s) VALUES(%s)"
        self.find = "SELECT * FROM %s WHERE %s = '%s'"
        self.conn = None
        self.cur = None
        self.connect()

    def __del__(self):
        self.close()

    def connect(self):
        self.conn = psycopg2.connect(
            host=self.host,
            database=self.database,
            user=self.user,
            password=self.password
        )
        self.cur = self.conn.cursor()

    def insert(self, table, **kwargs):
        names = list(kwargs.keys())
        values = list(kwargs.values())
        q = self.query % (table, ', '.join(names), ', '.join(["%s"] * len(names)))
        self.cur.execute(q, values)
        self.commit()

    def index(self, table, name, value):
        self.cur.execute(self.find % (table, name, value.replace("'", "''")))
        for row in self.cur.fetchall():
            return row[0]
        return -1

    def commit(self):
        self.conn.commit()

    def rollback(self):
        self.conn.rollback()

    def reset(self):
        self.rollback()
        self.commit()
        self.close()
        self.connect()

    def close(self):
        if self.conn is not None and self.conn.closed != 0:
            self.conn.close()
        if self.cur is not None:
            self.cur.close()
