import logging
import os

from tqdm import tqdm
from zipfile import ZipFile
from pymongo import MongoClient

MONGODB_USER = os.environ.get("MONGODB_USER")
MONGODB_PASS = os.environ.get("MONGODB_PASS")

class NoSQLDatabase:    
    
    # Configure mongodbd
    def __init__(self, cluster=None, db=None, collection=None, index = None, uri = None, k=None, candidates=None):
        self.cluster = cluster
        self.db = db
        self.collection = collection
        self.index = index
        self.uri = uri
        self.k = k
        self.candidates = candidates
    
    @classmethod
    def from_config(cls, config):
        config_conn = config["connection"]
        config_index = config["atlas_index"]
        uri = f"mongodb+srv://{MONGODB_USER}:{MONGODB_PASS}@BigDataFinalProject.sl03s.mongodb.net/?retryWrites=true&w=majority&appName=BigDataFinalProject"
        cluster = MongoClient(host=[uri])
        db = cluster[config_conn["database"]]
        collection = db[config_conn["collection"]]
        k = config["k"]
        candidates = config["candidates"]
        return cls(cluster=cluster, db=db, collection=collection, index = config_index, uri = uri, k=k, candidates=candidates)
    
    def set_index(self):
        self.collection.create_search_index(model=self.index)

    def upload(self, batch_id, batch_data):
        status_cnt = 0
        for payload in tqdm(batch_data, desc = f"Uploading processed documents for BATCH:{batch_id}"):
            # upload results 
            upload_result = payload.write.format("com.mongodb.spark.sql.DefaultSource")\
                .mode("append")\
                .option("database", "bigdata") \
                .option("collection", "newspapers")\
                .option("uri", self.uri)\
                .save()

    def query_keyword(self, query, pipeline, spark):      
        # process query
        df = spark.createDataFrame([{"FullText": query}])
        results = pipeline.fit(df).transform(df)
        values = results.toPandas().to_dict()
        ngrams = [v.asDict()["result"] for v in values["keywords"][0]] 
        
        # aggregate pipeline to find top-k articles based on input keywords
        data = self.collection.aggregate([
            {"$match": {"ObjectType": "Article"}}, # Filter only articles
            {"$project":{"keywords.result": 1, "keywords.metadata.score": 1}},
            {"$unwind": {"path": "$keywords", "preserveNullAndEmptyArrays": False}},
            {"$match": {"keywords.result":  {"$in": ngrams}}},
            {"$group": { "_id": "$_id", "keywords": {"$addToSet": "$keywords"}}},
            {"$unwind": {"path": "$keywords", "preserveNullAndEmptyArrays": False}},
            # TODO: Zero to large number
            {"$addFields": {"keywords.reciprocal": {"$divide": [1, {"$convert": {"input":"$keywords.metadata.score", "to": "double", "onError": 10**7, "onNull": 10**7}}]}}},
            {"$group": { "_id": "$_id", "score": {"$sum": "$keywords.reciprocal"},"keywords": {"$addToSet": "$keywords.result"}}},
            {"$sort": {"score": -1}},
            {"$limit": self.k}
        ])
            
        return data

    def query_embeddings(self, query, pipeline, spark):
        # process query
        df = spark.createDataFrame([{"FullText": query}])
        results = pipeline.fit(df).transform(df)
        values = results.toPandas().to_dict()
        vector = values["finished_embeddings"][0][0]

        # aggregate pipeline for vector search
        data = self.collection.aggregate([
                            {
                            "$vectorSearch": {
                                "index": "doc2vec_index",
                                "path": "finished_embeddings",
                                "queryVector": vector,
                                "numCandidates": self.candidates,
                                "limit": self.k,
    #                           "filter": {<filter-specification>}
                                }
                            }
        ])

        return data


    def query_hybrid(self, query, pipeline):
        # TODO: insert logic for hybrid search
        keyword_results = self.query_keyword(query, pipeline)
        embedding_results = self.query_embeddings(query, pipeline)
        hybrid_results = self.rrf(keyword_results, embedding_results)
        return hybrid_results

    def rrf(self, result_set1, result_set2):
        # TODO: insert logic for reciprocal rank fusion
        pass
