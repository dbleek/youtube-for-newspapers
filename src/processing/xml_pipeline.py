import pdb

import os 
import uuid
import shutil
import logging

from tqdm import tqdm
from zipfile import ZipFile
from pathlib import Path
from functools import reduce

import pyspark
from pyspark.sql import DataFrame
import sparknlp
from processing.nlp_pipeline import KeywordPipeline, EmbeddingsPipeline

class XmlPipeline:
    """A class representing an XML pipeline for performing batch uploads on Xml Documents.

    Attributes:
        batchsize (int): 
        doc2vec_config (dict):
        keyword_config (dict):
        data_raw (pathlib.Path):
        cache (bool):
        cache_dir (pathlib.Path):
        test (bool):
        zip2batch (dict):
        spark (pyspark.SparkConf):
        ddfs_batches (list of pysark.DataFrame):
    """

    def __init__(self, batchsize = None, doc2vec_config=None, keyword_config=None, data_raw=None, cache=None, cache_dir=None, test=None):
        """
        Initialize the XmlPipeline object.

        Args:
        """
        # init attrs
        self.batchsize = batchsize
        self.doc2vec_config = doc2vec_config
        self.keyword_config = keyword_config
        self.data_raw = data_raw
        self.cache = cache
        self.cache_dir = cache_dir
        self.test = test
        self.zip2batch = {}

        # processing attrs
        self.spark = None
        self.ddfs_batches = []
        self.yake_pipeline = None
        self.doc2vec_pipeline = None
    
    @classmethod
    def from_config(cls, config, args):
        """
        Instantiate XmlPipeline object from the config. Load in data both from the configuration file and arg paraser.

        Args:
            config (dict): Dictionary containing configuration arguments needed to run the system. 

        Returns:
            cls (XmlPipeline): Configured XmlPipeline object.
        """
        # config parameters
        batchsize = config["batchsize"]
        keyword_config = config["keywords"]
        doc2vec_config = config["doc2vec"]

        # args parameters
        data_raw = Path(args.data_raw)
        cache = args.cache
        cache_dir = Path(args.cache_dir)
        test = args.test
        
        return cls(batchsize = batchsize, doc2vec_config=doc2vec_config, keyword_config=keyword_config, data_raw=data_raw, cache = cache, cache_dir = cache_dir, test = test)
    
    def setup_spark(self):
        """Setup Spark context by setting config and creating context.
    
        Args:
            None.
    
        Returns:
            None. 
        """
        # setup Spark config
        conf = pyspark.SparkConf()
        conf.set('spark.jars.packages', 
                 "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1,com.databricks:spark-xml_2.12:0.18.0,com.johnsnowlabs.nlp:spark-nlp_2.12:5.3.3")
        conf.set('spark.driver.memory','8g')

        # create Spark context
        sc = pyspark.SparkContext(conf=conf)
        self.spark = pyspark.SQLContext.getOrCreate(sc)

    def extract_xml(self, zip_file): #fin ='250949924.xml'):
        """Load xml into spark dataframe.

        Args:
            zip_file (pathlib.Path): Path location for the zip file.

        Returns:
            ddf (pyspark.DataFrame): Distributed dataframe object version of the xml document.
        """
        
        # setup extraction
        zip_path = self.data_raw / zip_file
        tmp_dir = self.cache_dir / "tmp"
        tmp_dir.mkdir(exist_ok=True, parents=True)
        
        # extract zip file
        with ZipFile(zip_path, "r") as fzip:
            fzip.extractall(tmp_dir)

        return tmp_dir 

    def process_xml(self, xml_file): 
        """Load xml into spark dataframe.

        Args:
            zip_file (pathlib.Path): Path location for the zip file.

        Returns:
            ddf (pyspark.DataFrame): Distributed dataframe object version of the xml document.
        """
 
        # read zip file into distributed dataframe
        data = self.spark.read \
            .option('rootTag', 'Record')\
            .option('rowTag', 'Record')\
            .format("xml").load(str(self.cache_dir / "tmp" / xml_file ))
                
        # process keywords
        data_w_keywords = self.yake_pipeline.execute_pipeline(data)
            
        # process embeddings
        data_w_embeddings = self.doc2vec_pipeline.execute_pipeline(data_w_keywords)
            
        # append data
        pdb.set_trace()
        return data_w_keywords
        
      

    def setup_batch_jobs(self):
        """Method to setup batch jobs. Supports test and initializes logging. Splits files to upload into batches.

        Args:
            None.

        Returns:
            batches (list of str): List of zip file names separated into batches.
        """
        # setup batches
        list_of_zips = os.listdir(self.data_raw)
        batches = [list_of_zips[i: i + self.batchsize] for i in range(0, len(list_of_zips), self.batchsize)]

        # setup test support
        if self.test:
            batches = [batches[0]]

        # setup logging
        if self.cache:
            log_path = self.cache_dir / "logs" / "batch.log"
            log_path.parent.mkdir(exist_ok=True, parents=True)
            logging.basicConfig(
                filename= log_path,
                level=logging.INFO,
                format="%(asctime)s %(levelname)s %(message)s",
                datefmt = "%Y-%m-%d %H:%M:%S"
            )
        return batches

                                         
    def run_batch(self, batch):
        """Run batch job for xml pipeline.
        """
        
        processed_xml = []
        for zip_file in batch:
            # extract zip file and load into distributed data frame
            tmp_dir = self.extract_xml(zip_file)
            files_to_process = os.listdir(tmp_dir)
            
            for xml_file in tqdm(files_to_process, desc="Processing XML documents..."):
                processed_dict = self.process_xml(xml_file)
                processed_xml.append(processed_dict)
        
        # reduce processed data into single dataframe for batch
        #ddf_batch = reduce(DataFrame.unionAll, ddfs)

        # cleanup
        shututil.rmtree(tmp_dir)
         
        return processed_xml

    
    def cache_batch(self, batch, batch_data):
        """Cache batch job.

        Args:
            batch
            batch_data

        Returns:
            None.
        """
        # log processed data
        batch_id = uuid.uuid1()
        
        for zip_file in batch:
            self.zip2batch[zip_file] = batch_id
            logging.info(f"{batch_id} {zip_file}")

        # cache data
        batch_data.write.parquet(self.cache_dir / "data" / f"{batch_id}.parquet")
    
    def setup_nlp_pipelines(self):
        spark = sparknlp.start()

        #s process keywords
        self.yake_pipeline = KeywordPipeline.from_config(self.keyword_config)
        self.yake_pipeline.setup_pipeline()
            
        # process embeddings
        self.doc2vec_pipeline = EmbeddingsPipeline.from_config(self.doc2vec_config)
        self.doc2vec_pipeline.setup_pipeline()
    
    def batch_upload(self, db):
        """
        Args:
            db (NoSqlDatabase):

        Returns:
            None.
        """
        batches = self.setup_batch_jobs()
        self.setup_spark()
        self.setup_nlp_pipelines()

        # create batch jobs
        for batch in batches:
            batch_data = self.run_batch(batch)
            self.ddfs_batches.append(batch_data)
            
            if self.cache:
                self.cache_batch(batch, batch_data)
            
            db.upload(batch, batch_data)

        
