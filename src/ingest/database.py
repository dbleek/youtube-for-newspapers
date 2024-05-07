import logging
import os

from tqdm import tqdm
from zipfile import ZipFile
from pymongo import MongoClient

MONGODB_USER = os.environ.get("MONGODB_USER")
MONGODB_PASS = os.environ.get("MONGODB_PASS")

class NoSQLDatabase:    
    
    # Configure mongodbd
    def __init__(self, cluster=None, db=None, collection=None, index = None, uri = None, k=None, candidates=None, vector_penalty=None, keyword_penalty=None):
        self.cluster = cluster
        self.db = db
        self.collection = collection
        self.index = index
        self.uri = uri
        self.k = k
        self.candidates = candidates
        self.vector_penalty = vector_penalty
        self.keyword_penalty = keyword_penalty
    
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
        vector_penalty = config["vector_penalty"]
        keyword_penalty = config["keyword_penalty"]
        return cls(cluster=cluster, db=db, collection=collection, index = config_index, uri = uri, k=k, candidates=candidates, vector_penalty=vector_penalty, keyword_penalty=keyword_penalty)
    
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


    def query_hybrid(self, query, kw_pipeline, em_pipeline, spark):
        # TODO: insert logic for hybrid search
        #keyword_results = self.query_keyword(query, pipeline)
        #embedding_results = self.query_embeddings(query, pipeline)
        #hybrid_results = self.rrf(keyword_results, embedding_results)

        hybrid_results = self.collection..aggregate([
            {
                "$vectorSearch": {
                    "index": "doc2vec_index",
                    "path": "finished_embeddings",
                    "queryVector": vector,
                    "numCandidates": 100,
                    "limit": self.k
                }
            },
            {"$group": {"_id": 0, "docs": {"$push": "$$ROOT"}}}, 
            {"$unwind": {"path": "$docs", "includeArrayIndex": "rank"}}, 
            {"$addFields": {"vs_score": {"$divide": [1.0, {"$add": ["$rank", self.vector_penalty, 1]}]}}}, 
            {"$project": {"vs_score": 1, "_id": "$docs._id","recordID": "$docs.RecordID"}},    
            {
                "$unionWith": {
                    "coll": "newspapers",
                    "pipeline": [
                        {"$match": {"ObjectType": "Article"}}, # Filter only articles
                        {"$project":{"keywords.result": 1, "keywords.metadata.score": 1, "RecordID": 1}},
                        {"$unwind": {"path": "$keywords", "preserveNullAndEmptyArrays": False}},
                        {"$match": {"keywords.result":  {"$in": query}}},
                        {"$group": { "_id": "$_id", "keywords": {"$addToSet": "$keywords"}, "recordID": {"$first": "$RecordID"}}},
                        {"$unwind": {"path": "$keywords", "preserveNullAndEmptyArrays": False}},
                        # TODO: Zero to large number
                        {"$addFields": {"keywords.reciprocal": {"$divide": [1, {"$convert": {"input":"$keywords.metadata.score", "to":"double", "onError": 10**7, "onNull": 10**7}}]}}},
                        {"$group": { "_id": "$_id", "score": {"$sum": "$keywords.reciprocal"}, "recordID": {"$first": "$recordID"}}},
                        {"$sort": {"score": -1}},
                        {"$limit": self.k},
                        {"$addFields": {"kw_score": {"$divide": [1.0, {"$add": ["$score", self.keyword_penalty, 1]}]}}},
                        {"$project": {"kw_score": 1,"_id": 1, "recordID": 1}}
                      ]
                }
            },
            {
                "$group": {
                    "_id": "$_id",
                    "recordID": {"$first": "$recordID"},
                    "vs_score": {"$max": "$vs_score"},
                    "kw_score": {"$max": "$kw_score"}
                }
            },
            {
                "$project": {
                  "_id": 1,
                  "recordID": 1,
                  "vs_score": {"$ifNull": ["$vs_score", 0]},
                  "kw_score": {"$ifNull": ["$kw_score", 0]}
                }
            },
            {
                "$project": {
                    "score": {"$add": ["$kw_score", "$vs_score"]},
                    "_id": 1,
                    "recordID": 1,
                    "vs_score": 1,
                    "kw_score": 1
                }
            },
          {"$sort": {"score": -1}},
          {"$limit": self.k}   
        ])
        
        return hybrid_results

    def rrf(self, result_set1, result_set2):
        # TODO: insert logic for reciprocal rank fusion
        pass
