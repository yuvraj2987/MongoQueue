

class Queue():

    def __init__(self, mongo_client, collection_name):
        self.db = mongo_client.messages
        self.collection = self.db[collection_name]
        #keys = {"item":1, "_id":1}
        #self.collection.ensure_index(keys)

    def get(self):
        collection = self.collection
        doc = collection.find_and_modify({"sort":{"_id":1}, "remove":True})
        return doc["item"]
    
    def add(self, item):
        collection = self.collection
        collection.insert({"item":item})
