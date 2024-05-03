# import modules
import os
import json
import getpass
import pyspark
from pathlib import Path

from processing import xml_pipeline
from io import database

# set global vars
ROOT_DIR = Path.cwd()
USER = getpass.getuser()
os.environ.get("MONGODB_USER")
os.environ.get("MONGODB_PASS")

def read_config(fin):
    """
    Read .json config file
    """
    with open('data.json') as f:
        config = json.load(f)
    return config

def create_arg_parser():
    """Create argument parser to set application run parameters.

    Args:
        None.

    Returns:
        argument_parser (argparse.ArgumentParser): Argument parser object to pass to run function.
    """
    argument_parser = ArgumentParser(description= "Youtube for Newspapers Search")
    argument_parser.add_argument(
        "--config_path",
        type = str,
        default = f"{ROOT_DIR}/config.json"
    )
    argument_parser.add_argument(
        "--data_raw",
        type = str,
        default = "/scratch/work/public/proquest/proquest_hnp/BostonGlobe/BG_20151210212722_00001.zip"
    )
    argument_parser.add_argument(
        "--cache",
        action="store_true",
        help = "If true, cache directory names loaded into mongodb."
    )
    argument_parser.add_argument(
        "--cache_dir",
        default = f"/scratch/{USER}/youtube-for-newspapers-cache",
        help = "Location to cache processed data for youtube for newspapers"
    )
    argument_parser.add_argument(
        "--test",
        action="store_true",
        help = "If true, test processing pipeline on the first zip file in the `data_raw` dir." 
    )
    return argument_parser

def run(args):
    """
    """
    # load configs
    config = read_config(args.config_path)
    database_config = config["nosql_database"]
    processing_config = config["processing_pipeline"]
    
    # setup database connection
    db = database.NoSQLDatabase.from_config(database_config)
    db.set_index()
    
    # run processing batch job
    pipe = xml_pipeline.XmlPipeline.from_config(processing_config, args)
    pipe.batch_upload(db=db)
    
    
def main():
    args = create_arg_parser().parse_args()
    run(args)
    
if __name__ == "__main__":
    main()
