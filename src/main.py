import os
import json
from pathlib import Path
import pyspark

from processing import database, xml_pipeline

ROOT_DIR = Path.cwd()

os.environ.get("MONGODB_USER")
os.environ.get("MONGODB_PASS")


def read_config(fin):
    """
    Read .json config file
    """
    with open('data.json') as f:
        config = json.load(f)
    return config


def load_xml(spark, fin ='250949924.xml'):
    source_path = "/scratch/work/public/proquest/proquest_hnp/BostonGlobe/BG_20151210212722_00001.zip"
    with ZipFile(source_path, "r") as fzip:
        print(fzip.infolist())
        fzip.extract(fin , 'zip_tmp')
    df = spark.read \
        .option('rootTag', 'Record')\
        .option('rowTag', 'Record')\
        .format("xml").load(f"zip_tmp/{fin}.xml")
    return df


def create_arg_parser():
    argument_parser = ArgumentParser(description= "Youtube for Newspapers Search")
    argument_parser.add_argument(
        "--mode",
        type = str,
        choices = ["process", "query"],
        default = "process",
        help = "Process or query the search engine"
    )
    argument_parser.add_argument(
        "--config_path",
        type = str,
        default = ROOT_DIR / "config.json"
    )
    return argument_parser
    
def setup_spark():
    # setup Spark config
    conf = pyspark.SparkConf()
    conf.set('spark.jars.packages', 
             "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1,com.databricks:spark-xml_2.12:0.18.0,com.johnsnowlabs.nlp:spark-nlp_2.12:5.3.3")
    conf.set('spark.driver.memory','8g')
    sc = pyspark.SparkContext(conf=conf)
    spark = pyspark.SQLContext.getOrCreate(sc)
    return spark
    
def run(args):
    # load configs
    config = read_config(args.config_path)
    database_config = config["nosql_database"]
    processing_config = config["processing_pipeline"]
    
    # setup database connection
    db = database.NoSQLDatabase.from_config(database_config)
    db.set_index()
    
    #load and process data
    spark = setup_spark()
    data = load_xml(spark)
    
    # process keywords
    yake_pipeline = xml_pipeline.YakeKeywordPipeline.from_config(processing_config["keywords"])
    yake_pipeline.setup_pipeline()
    data_w_keywords = yake_pipeline.execute_pipeline(data)
    
    # process embeddings
    doc2vec_pipeline = xml_pipeline.Doc2VecEmbeddingsPipeline.from_config(processing_config["doc2vec"])
    doc2vec_pipeline.setup_pipeline()
    data_w_embeddings = doc2vec_pipeline.execute_pipeline(data_w_keywords)
    
def main():
    args = create_arg_parser().parse_args()
    run(args)
    
if __name__ == "__main__":
    main()