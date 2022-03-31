from pymongo import MongoClient


class Entry:
    def __init__(self, entry):
        self.entry = entry

    def var(self, path: str):
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
    def __init__(self, mongo, name):
        self.collection = mongo[name]

    def for_each(self, f):
        for var in self.collection.find():
            f(Entry(var))


class Mongo:
    def __init__(self, link="localhost:27017", database="huwebshop"):
        self.client = MongoClient(f"mongodb://{link}/")
        self.db = self.client[database]

    def __del__(self):
        self.client.close()

    def get_collection(self, collection) -> Collection:
        return Collection(self.db, collection)

    def close(self):
        self.__del__()
