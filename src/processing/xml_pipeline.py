import pickle
import os 
import uuid
import shutil
import logging
import random
import zipfile

from itertools import groupby
from tqdm import tqdm
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

    def __init__(self, batchsize = None, samplesize = None, random_state =None, doc2vec_config=None, keyword_config=None, data_raw=None, cache=None, cache_dir=None, test=None, runtime=None):
        """
        Initialize the XmlPipeline object.

        Args:
        """
        # init attrs
        self.runtime = runtime
        self.random_state = random_state
        self.batchsize = batchsize
        self.samplesize = samplesize
        self.doc2vec_config = doc2vec_config
        self.keyword_config = keyword_config
        self.data_raw = data_raw
        self.cache = cache
        self.cache_dir = cache_dir
        self.test = test
        
        # processing attrs
        self.spark = None
        self.yake_pipeline = None
        self.doc2vec_pipeline = None
        self.tmp_dir = None
    
    @classmethod
    def from_config(cls, config, args, runtime):
        """
        Instantiate XmlPipeline object from the config. Load in data both from the configuration file and arg paraser.

        Args:
            config (dict): Dictionary containing configuration arguments needed to run the system. 

        Returns:
            cls (XmlPipeline): Configured XmlPipeline object.
        """
        # config parameters
        random_state = config["random_state"]
        batchsize = config["batchsize"]
        samplesize = config["samplesize"]
        keyword_config = config["keywords"]
        doc2vec_config = config["doc2vec"]
        
        # args parameters
        data_raw = Path(args.data_raw)
        cache = args.cache
        cache_dir = Path(args.cache_dir)
        test = args.test
        
        return cls(random_state = random_state, batchsize = batchsize, samplesize = samplesize, doc2vec_config=doc2vec_config, keyword_config=keyword_config, data_raw=data_raw, cache = cache, cache_dir = cache_dir, test = test, runtime=runtime)
    
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
    
    def sample_zip_files(self, list_of_zips):
        """
        Args:
            list_of_zips
            samplesize
        """
        # calc sample size per zipfile
        nzips = len(list_of_zips)
        samplesize_per_zip = self.samplesize // nzips
        samplesize_rem = self.samplesize % nzips
        
        # iterate over zip files and sample xml
        xml_to_process = []
        for ii, zip_file in tqdm(enumerate(list_of_zips), total = nzips, desc="Sampling Zip Files"):
            zip_path = self.data_raw / zip_file
            with zipfile.ZipFile(zip_path, "r") as zf:
                if ii == len(list_of_zips):
                    samplesize_per_zip += samplesize_rem
                list_of_xml = zf.namelist()
                xml_sample = random.sample(list_of_xml, min(samplesize_per_zip, len(list_of_xml)))
                xml_to_process.extend([(zip_file, xml_file) for xml_file in xml_sample])
        
        return xml_to_process
    
    def extract_xml(self, batch):
        """Load xml into spark dataframe.

        Args:
            zip_file (pathlib.Path): Path location for the zip file.

        Returns:
            ddf (pyspark.DataFrame): Distributed dataframe object version of the xml document.
        """
        # setup tmp dir
        tmp_dir = self.cache_dir / "tmp" / self.runtime
        tmp_dir.mkdir(exist_ok=True, parents=True)

        # perform zip file extraction
        get_zip_file = lambda x: x[0]
        sort_batch = sorted(batch, key=get_zip_file)
        for zip_file, item in groupby(batch, get_zip_file):
            zip_path = self.data_raw / zip_file
            with zipfile.ZipFile(zip_path, "r") as fzip:
                for _, xml_file in item:
                    fzip.extract(xml_file, path=tmp_dir)
        
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
        
        return data_w_embeddings
    
    def setup_logging(self):
        log_path = self.cache_dir / "logs" / "batch.log"
        log_path.parent.mkdir(exist_ok=True, parents=True)
        logging.basicConfig(
            filename= log_path,
            level=logging.INFO,
            format="%(asctime)s %(levelname)s %(message)s",
            datefmt = "%Y-%m-%d %H:%M:%S"
        )
    
    def setup_batch_jobs(self):
        """
        Create batchjobs from 
                                      
        Args:
            None.

        Returns:
            batches (list of tuples): tuple of zipfile name and xml file name.
        """
        random.seed(self.random_state)
        list_of_zips = os.listdir(self.data_raw)
        xml_to_process = self.sample_zip_files(list_of_zips)
        xml_batches = { uuid.uuid1(): xml_to_process[i: i + self.batchsize] for i in range(0, len(xml_to_process), self.batchsize)}
        
        # setup test support
        if self.test:
            batch_id = sorted(list(xml_batches.keys()))[0]
            xml_batches = {batch_id: xml_batches[batch_id]}

        # setup logging
        self.setup_logging()
        
        return xml_batches
    
    
    def run_batch(self, batch_id, batch):
        ddfs = []
        self.tmp_dir = self.extract_xml(batch)
        for zip_file, xml_file in tqdm(batch, desc=f"Processing XML documents for BATCH:{batch_id}"):
            try:
                processed_xml = self.process_xml(xml_file)
                ddfs.append(processed_xml)
            except:
                logging.info(f"{batch_id} {xml_file} FAILED")
        
        return ddfs
    
    
    def cache_batch(self, batch_id, batch_data):
        """Cache batch job.

        Args:
            batch
            batch_data

        Returns:
            None.
        """
        data_dir = self.cache_dir / "data"
        data_dir.mkdir(exist_ok=True, parents=True)
        
        print(f"Caching processed documents for BATCH:{batch_id}")
        
        # cache processed data
        with open(data_dir / f"{batch_id}.pkl", "wb") as handler:
            pickle.dump(batch_data, handler, protocol=pickle.HIGHEST_PROTOCOL)
           
        
    
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
        for batch_id, batch in batches.items():
            # run & upload batch
            batch_data = self.run_batch(batch_id, batch)
            logging.info(f"{batch_id} PROCESSED")
            
            if self.cache:
                self.cache_batch(batch_id, batch_data)
                logging.info(f"{batch_id} CACHED")
            
            db.upload(batch_id, batch_data)
            logging.info(f"{batch_id} UPLOADED")
            
            # cleanup
            shutil.rmtree(self.tmp_dir)
            self.spark.clearCache()

    @staticmethod
    def process_payload(payload):
        payload_res = payload.toPandas().to_dict(orient="records")[0]
        return payload_res

    

        

