import psycopg2


class Postgres:
    """
    Psycopg2 wrapper class
    """

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
        self.commit_counter = 0
        self.connect()

    def __del__(self):
        self.close()

    def connect(self):
        """
        Start the Postgresql connection
        """

        self.conn = psycopg2.connect(
            host=self.host,
            database=self.database,
            user=self.user,
            password=self.password
        )
        self.cur = self.conn.cursor()

    def insert(self, table, **kwargs):
        """
        Insert a row into a table
        :param table: The table to insert to
        :param kwargs: A map of keys and values for column names and values
        """

        names = list(kwargs.keys())
        values = list(kwargs.values())
        q = self.query % (table, ', '.join(names), ', '.join(["%s"] * len(names)))
        self.cur.execute(q, values)
        self.commit_counter += 1
        if self.commit_counter > 1000:
            self.commit_counter = 0
            self.commit()

    def index(self, table, name, value):
        """
        Get the row index of an entry
        :param table: The table to search
        :param name: The column name to search
        :param value: The value to search
        :return: The row index of the value or -1 if not found
        """

        self.cur.execute(self.find % (table, name, value.replace("'", "''")))
        for row in self.cur.fetchall():
            return row[0]
        return -1

    def execute(self, query):
        """
        Execute an sql query
        :param query: The query to execute
        """

        self.cur.execute(query)
        self.commit()

    def commit(self):
        """
        Commit any made changes to the database
        """
        self.conn.commit()

    def close(self):
        """
        Close the Postgresql connection
        """

        if self.conn is not None and self.conn.closed != 0:
            self.conn.close()
        if self.cur is not None:
            self.cur.close()
