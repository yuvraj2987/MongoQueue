


class MongoQueue():

    def __init__(self, mongo_client, queue_name):
        self.client = mongo_client
        self.db = mongo_client.messages
        self.coll = self.db[queue_name]
    

    def add(self, item):
        #self.client.write_concern={"w":1}
        self.coll.insert({"item":item})

    def get(self):
        
        #self.client.write_concern = {"w":1}
        doc = self.coll.find_and_modify(query={}, sort={"_id":1}, remove=True)
        if doc is None:
            return None
        #else:
        return doc["item"]

    def drop(self):
        """
        return True if this collection is dropped successfully
        """
        return self.db.drop_collection(self.coll)
