# YouTube for Newspapers

[![Python Version](https://img.shields.io/badge/python-3.9.16-blue.svg)](https://python.org)

Big Data Spring 2024 Final Project

## System Overview

`Youtube for Newspapers` is a system for doing batch processing and upload of Proquest XML documents to MongoDB atlas. The system supports processing for keyword, vector, and hybrid search.

To run the system:

1. Log into vpn.nyu.edu and start up Greene shell access.
- Copy repo to /scratch/USER

2. Install and activate the conda environment.
- Please follow instructions on HPC website (https://sites.google.com/nyu.edu/nyu-hpc/hpc-systems/greene/software/conda-environments-python-r)
- Packages to install listed in env/environment.yaml

```bash
cd /scratch/USER
module purge
module load anaconda3/2020.07
conda env create -p ./bdenv -f /scratch/$USER/youtube-for-newspapers/env/environment.yaml
conda activate /scratch/USER/bdenv
```
3. Save environment variables.
- MONGODB_USER  & MONGODB_PASS

4. Run src/main.py
```bash
/scratch/jp6203/bdenv/bin/python src/main.py --cache --test
```
- `--config_path`: Specifies path to configuration file.
- `--data_raw`: Specifies path to Proquest xml zip file locations (e.g. /scratch/work/public/proquest/proquest_hnp/BostonGlobe).
- `--cache`: If true, cache processed data in parquet files on scratch and create system logs.
- `--cache_dir`: Path to cache processed data (e.g. /scratch/{USER}/youtube-for-newspapers-cache).
- `--test`: If true, only run batch upload for first batch.

## Configuration

Configuration arguments for system.

```json
{
    "processing_pipeline": {
	"batchsize" : Number of documents to process in a batch,
        "keyword" : {
            "min_ngrams": Minimum ngrams for YAKE keyword,
            "max_ngrams": Maximum ngrams for YAKE keyword,
            "num_keywords": Number of keywords generated per XML file,
            "context_chars": Characters that separate context for stopword splitting
        },
        "doc2vec": {
            "pretrained_model" : {
                "name" : String encoding doc2vec model type,
                "lang": Language doc2vec model was trained on.
            },
            "vector_size" : Vector size for doc2vec model,
            "vector_output": Whether to output vectors
        }
    },
    "nosql_database" : {
        "connection" : {
            "database": Name of database to connect to,
            "collection": Name of collection to perform batch upload to
        },
        "atlas_index" : {
            "mappings": {
                "fields": {
                    "finished_embeddings": [
                        {
                        "dimensions": Vector embedding dimension for vector search,
                        "similarity": Distance measure for computing similarity,
                        "type": ANN Similarity type
                        }
                    ]
                }
            }
        }
    }
}
```
