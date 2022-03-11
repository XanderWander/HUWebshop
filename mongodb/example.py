from pymongo import MongoClient

client = MongoClient("mongodb://localhost:27017/")
db = client.huwebshop

no_count = 0
for idx, product in enumerate(db.products.find()):
    flavor = product.get('size')
    if flavor is not None:
        print(product)
        break
    else:
        no_count += 1

print(no_count)

