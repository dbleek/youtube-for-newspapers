import uuid
import shututil
import logging
from functools import reduce

import pyspark
from pyspark.sql import DataFrame

from nlp_pipeline import KeywordPipeline, EmbeddingsPipeline
from database

class XmlPipeline:
    """
    """

    def __init__(self, batchsize = None, doc2vec_config=None, keyword_config=None, data_dir=None, cache=None, cache_dir=None, test=None):
        # init attrs
        self.batchsize = batchsize
        self.doc2vec_config = doc2vec_config
        self.keyword_config = keyword_config
        self.data_dir = data_dir
        self.cache = cache
        self.cache_dir = cache_dir
        self.test = test
        self.zip2batch = {}

        # processing attrs
        self.spark = None
        self.ddfs_batches = []

    def from_config(cls, config, args):
        """
        """
        # config parameters
        batchsize = config["batchsize"]
        keyword_config = config["keywords"]
        doc2vec_config = config["doc2vec"]

        # args parameters
        data_dir = args.data_dir
        cache = args.cache
        cache_dir = args.cache_dir
        test = args.test
        
        return cls(batchsize = batchsize, doc2vec_config=doc2vec_config, keyword_config=keyword_config, data_dir=data_dir, cache = cache, cache_dir = cache_dir, test = test)
    
    def setup_spark():
        """Setup Spark context by setting config and creating context.
    
        Args:
            None.
    
        Returns:
    
        """
        # setup Spark config
        conf = pyspark.SparkConf()
        conf.set('spark.jars.packages', 
                 "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1,com.databricks:spark-xml_2.12:0.18.0,com.johnsnowlabs.nlp:spark-nlp_2.12:5.3.3")
        conf.set('spark.driver.memory','8g')

        # create Spark context
        sc = pyspark.SparkContext(conf=conf)
        self.spark = pyspark.SQLContext.getOrCreate(sc)

    def load_xml(zip_file): #fin ='250949924.xml'):
        """Load xml into spark dataframe.
        """
        # setup extraction
        zip_dir = zip_file.parent
        fin = zip_file.parts[-1]
        tmp_dir = self.cache_dir / "tmp"
        tmp_dir.mkdir(exist_ok=True, parents=True)
        
        # extract zip file
        with ZipFile(zip_dir, "r") as fzip:
            print(fzip.infolist())
            fzip.extract(fin , tmp_dir)

        # read zip file into distributed dataframe
        ddf = self.spark.read \
            .option('rootTag', 'Record')\
            .option('rowTag', 'Record')\
            .format("xml").load(f"tmp/{fin}.xml")

        # cleanup
        shututil.rmtree(tmp_dir)
        
        return ddf

    def setup_batch_jobs():

        # setup batches
        list_of_zips = os.listdir(self.data_dir)
        batches = [list_of_zips[i: i + self.batchsize] for targe range(0, len(list_of_zips), self.batchsize)]

        # setup test support
        if self.test:
            batches = [batches[0]]

        # setup logging
        if self.cache:
            logging.basicConfig(
                filename= self.cache_dir / "logs" / "batch.log",
                level=logging.INFO,
                format="%(asctime)s %(levelname)s %(message)s",
                datefmt = "%Y-%m-%d %H:%M:%S"
            )
        return batches

                                         
    def run_batch(self, batch):
        """Run batch job for xml pipeline.
        
        """
        
        ddfs = []
        for zip_file in batch:
            # extract zip file and load into distributed data frame
            data = load_xml(zip_file)
            
            # process keywords
            yake_pipeline = KeywordPipeline.from_config(self.keywords_config)
            yake_pipeline.setup_pipeline()
            data_w_keywords = yake_pipeline.execute_pipeline(data)
            
            # process embeddings
            doc2vec_pipeline = EmbeddingsPipeline.from_config(self.doc2vec_config)
            doc2vec_pipeline.setup_pipeline()
            data_w_embeddings = doc2vec_pipeline.execute_pipeline(data_w_keywords)

            # append data
            ddfs.append(data_with_embeddings)

        # reduce processed data into single dataframe for batch
        ddf_batch = reduce(DataFrame.unionAll, ddfs)
        return ddf_batch

    
    def cache_batch(batch, batch_data):
        """Cache batch job.
        
        """
        # log processed data
        batch_id = uuid.uuid1()
        
        for zip_file in batch:
            self.zip2batch[zip_file] = batch_id
            logging.info(f"{batch_id} {zip_file}")

        # cache data
        batch_data.write.parquet(self.cache_dir / "data" / f"{batch_id}.parquet")

    def batch_upload(db):
        """
        """
        batches = self.setup_batch_jobs()
        
        # create batch jobs
        for batch in batches:
            batch_data = self.run_batch(batch)
            self.ddfs_batches.append(batch_data)
            
            if self.cache:
                self.cache_batch(batch, batch_data)
            
            payload = self.create_payload(batch_data)
            db.upload(payload)

        
