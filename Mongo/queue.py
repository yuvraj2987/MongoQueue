


class MongoQueue():

    def __init__(self, mongo_client, queue_name):
        self.db = mongo_client.messages
        self.coll = self.db[queue_name]

    def add(self, item):
        self.coll.insert({"item":item})

    def get(self):
        
        doc = self.coll.find_and_modify(query={}, sort={"_id":1}, remove=True)
        return doc["item"]

