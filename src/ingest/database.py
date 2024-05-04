import logging
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
    
    def set_index(self):
        self.collection.create_search_index(self.index)

    def upload(self, batch, payload):
        upload_result = self.collection.insert_many(payload)
        
        if upload_result.modified_count == len(payload):
            status = "SUCCESS"
        else:
            status = "FAIL"

        logging.info(f"{batch} {status}")

    def query_keyword(self, query, pipeline):
        processed_query = pipeline.execute_light_pipeline(query)
        # TODO: inject logic for keyword search
        pass

    def query_embeddings(self, query, pipeline):
        processed_query = pipeline.execute_light_pipeline(query)
        # TODO: insert logic for embedding search
        pass

    def query_hybrid(self, query, pipeline):
        # TODO: insert logic for hybrid search
        keyword_results = self.query_keyword(query, pipeline)
        embedding_results = self.query_embeddings(query, pipeline)
        hybrid_results = self.rrf(keyword_results, embedding_results)
        return hybrid_results

    def rrf(self, result_set1, result_set2):
        # TODO: insert logic for reciprocal rank fusion
        pass
