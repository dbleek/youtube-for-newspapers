import pdb
from tqdm import tqdm
import logging
import os        
from zipfile import ZipFile
from pymongo import MongoClient

MONGODB_USER = os.environ.get("MONGODB_USER")
MONGODB_PASS = os.environ.get("MONGODB_PASS")

class NoSQLDatabase:    
    
    # Configure mongodbd
    def __init__(self, cluster=None, db=None, collection=None, index = None, uri = None):
        self.cluster = cluster
        self.db = db
        self.collection = collection
        self.index = index
        self.uri = uri
    
    @classmethod
    def from_config(cls, config):
        config_conn = config["connection"]
        config_index = config["atlas_index"]
        uri = f"mongodb+srv://{MONGODB_USER}:{MONGODB_PASS}@bigdatafinalproject.sl03s.mongodb.net/?retryWrites=true&w=majority&appName=BigDataFinalProject"
        cluster = MongoClient(host=[uri])
        db = cluster[config_conn["database"]]
        collection = db[config_conn["collection"]]
        return cls(cluster=cluster, db=db, collection=collection, index = config_index, uri = uri)
    
    def set_index(self):
        self.collection.create_search_index(model=self.index)

    def upload(self, batch, batch_payload):
        status_cnt = 0
        for payload in tqdm(batch_payload, desc = "Uploading Batch..."):
            # upload results 
            upload_result = payload.write.format("com.mongodb.spark.sql.DefaultSource")\
                .mode("overwrite")\
                .option("database", "bigdata") \
                .option("collection", "newspapers")\
                .option("uri", self.uri)\
                .save() 
            
            #upload_result = self.collection.insert_one(payload)
        #     if upload_result.modified_count == len(payload):
        #         status = 1
        #     else:
        #         status = 0
        #     status_cnt += status 
        
        # logging.info(f"{batch} BATCHSIZE={len(batch_payload)} STATUSCOUNT={status_cnt}")

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
