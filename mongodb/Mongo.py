from pymongo import MongoClient


class Entry:
    """
    MongoDB single collection entry instance
    """

    def __init__(self, entry):
        self.entry = entry

    def var(self, path: str):
        """
        Get a variable from this entry
        :param path: The path to the variable e.g 'product.name' for product>name
        :return: The variable from MongoDB
        """

        data = path.split(".")
        if len(data) == 0:
            return None
        target = self.entry
        for step in data:
            target = target.get(step)
            if target is None:
                return None
        return target


class Collection:
    """
    MongoDB's collection instance
    """

    def __init__(self, mongo, name):
        self.collection = mongo[name]

    def for_each(self, f, limit=-1):
        """
        Call a function for each row with the Entry argument
        :param f: The function to call e.g 'product(entry: Entry)'
        :param limit: Optional limit of rows
        """

        c = 0
        for var in self.collection.find():
            f(Entry(var))
            c += 1
            if c == limit:
                return


class Mongo:
    """
    MongoDB's connection instance
    """

    def __init__(self, link="localhost:27017", database="huwebshop"):
        self.client = MongoClient(f"mongodb://{link}/")
        self.db = self.client[database]

    def __del__(self):
        self.client.close()

    def get_collection(self, collection) -> Collection:
        """
        Get a collection from the MongoDB database
        :param collection: The collection to get
        :return: The collection wrapped in the Collection object
        """

        return Collection(self.db, collection)

    def close(self):
        """
        Close the current MongoDB connection
        """

        self.__del__()
