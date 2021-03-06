import sys
import logging


class MongoQueue():

    def __init__(self, mongo_client, queue_name):
        self.client = mongo_client
        self.name = queue_name
        self.db = mongo_client.messages
        self.coll = self.db[queue_name]
    

    def add(self, item):
        logging.info("--- %s MongoQueue.add() ----", self.name)
        try:
            #self.client.write_concern={"w":1}
            item_id = self.coll.insert({"item":item})
            logging.debug("Insert item in MongoQueue with id=%s", item_id)

        except pymongo.error.AutoReconnect:
            logging.warning("Db connection lost... trying to reconnect")
        except:
            logging.error("Unhandled exceptioin.... unable to insert ")
            e = sys.exc_info()
            logging.warning("Exc Info:%s", str(e))

    def get(self):
        
        #self.client.write_concern = {"w":1}
        try:
            logging.info("--- %s MongoQueue.get() ---", self.name)
            doc = self.coll.find_and_modify(query={}, sort={"_id":1}, remove=True)
            if doc is None:
                logging.info(" %s MongoQueue is Empty", self.name)
                return None
            #else:
            logging.debug("removing element with id=%s", doc["_id"])
            return doc["item"]
        except:
            logging.error("Unhandled exception... unable to get item from the queue")
            e = sys.exc_info()
            logging.warning("Exc Info:%s", str(e))

    def drop(self):
        """
        return True if this collection is dropped successfully
        """
        try:
            logging.info("--- MongoQueue.drop() ---")
            logging.info("%s Dropped", self.name)
            return self.db.drop_collection(self.coll)
        except:
            logging.error("Unhandled exception")
            e = sys.exc_info()
            logging.warning("Exc Info:%s", str(e))
