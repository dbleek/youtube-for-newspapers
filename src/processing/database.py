import os        
from zipfile import ZipFile
from pymongo import MongoClient

MONGODB_USER = os.environ.get("MONGODB_USER")
MONGODB_PASS = os.environ.get("MONGODB_PASS")

class NoSQLDatabase:    
    
    # Configure mongodbd
    def init(self, cluster=None, db=None, collection=None, index = None):
        self.cluster = cluster
        self.db = db
        self.collection = collection
        self.index = index
    
    @classmethod
    def from_config(cls, config):
        config_conn = config["connection"]
        config_index = config["index"]
        mongodb_conn = f"mongodb+srv://{MONGODB_USER}:{MONGODB_PASS}@hybrid-search-test.owlo8k4.mongodb.net/"
        cluster = MongoClient(host=[mongodb_conn])
        db = cluster[config_conn["database"]]
        collection = db[config_conn["collection"]]
        return cls(cluster=cluster, db=db, collection=collection, index = config_index)
    
    def set_index(self)
        self.collection.create_search_index(self.index)



        