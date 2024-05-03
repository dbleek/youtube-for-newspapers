import pyspark
from pyspark.ml import Pipeline

from sparknlp.annotator import StopWordsCleaner, DocumentAssembler, SentenceDetector, EmbeddingsFinisher
from sparknlp.annotator import Doc2VecModel, YakeKeywordExtraction

class YakeKeywordPipeline(Pipeline):
    """
    A pipeline for extracting keywords using YAKE.

    Example Usage:
        pipeline = YakePipeline()
        processed_df = pipeline.fit(df).transfrom(df)
    """
    def __init__(self, stopwords=None, document_assembler=None, sentence_detector=None tokenizer=None, keywords=None):
        super(KeywordPipeline, self).__init__()
        self.stopwords = stopwords
        self.document_assembler = document_assembler
        self.sentence_detector = sentence_detector
        self.tokenizer = tokenizer
        self.keywords = keywords
    
    @classmethod
    def from_config(cls, config):
        """
        Create yake keyword extraction pipeline initialized from a configuration file
        """
        stopwords = StopWordsCleaner().getStopWords()
        document_assembler = DocumentAssembler() \
            .setInputCol("text")
            .setOutputCol("keyword_document")
        sentence_detector = SentenceDetector() \
            .setInputCols(["keyword_document"]) \
            .setOutputCol("keyword_sentence")
        tokenizer = Tokenizer() \
            .setInputCols(["keyword_sentence"]) \
            .setOutputCol("keyword_token") \
            .setContextChars(config["context_chars"])
        keywords = YakeKeywordExtraction() \
            .setInputCols(["keyword_token"]) \
            .setOutputCol("keywords") \
            .setMinNGrams(config["min_ngrams"]) \
            .setMaxNGrams(config["max_ngrams"]) \
            .setNKeywords(config["num_keywords"]) \
            .setStopWords(stopwords)
        return cls(stopwords=stopwords, document_assembler=document_assembler, sentence_detector=sentence_detector, tokenizer=tokenizer, keywords=keywords)
    
    def setup_pipeline(self):
        """
        """
        self.setStages([self.document, self.sentence_detector, self.token, self.keywords])
    
    def execute_pipeline(self, data):
        """
        """
        res = self.fit(data)
        return res
        
class Doc2VecEmbeddingsPipeline(Pipeline):
    
    def __init__(self, stopwords=None, document_assembler=None, sentence_detector=None tokenizer=None, keywords=None):
        super(EmbeddingsPipeline, self).__init__()
        self.stopwords = stopwords
        self.document_assembler = document_assembler
        self.sentence_detector = sentence_detector
        self.tokenizer = tokenizer
        self.keywords = keywords
    
    @classmethod
    def from_config(cls, config):
        """
        Create yake keyword extraction pipeline initialized from a configuration file
        """
        pretrained_config = config["pretrained_model"]
        document_assembler = DocumentAssembler() \
            .setInputCol("text") \
            .setOutputCol("embedding_document")
        tokenizer = Tokenizer() \
            .setInputCols("embedding_document") \
            .setOutputCol("embedding_token")
        embeddings = Doc2VecModel.pretrained(
            name = pretrained_config["name"],
            lang = pretrained_config["name"]
        ) \
            .setInputCols(["embedding_token"]) \
            .setOutputCol("embeddings")
        embeddings_finisher = EmbeddingsFinisher() \
            .setInputCols(["embeddings"]) \
            .setOutputCols("finished_embeddings") \
            .setOutputAsVector(config["vector_output"]) \
            .setVectorSize(config["vector_size"])
    
    def setup_pipeline(self):
        self.setStages([
            self.document_assembler,
            self.tokenizer,
            self.embeddings,
            self.embeddings_finisher
        ])
        
    def execute_pipeline(self, data)
        """
        """
        res = self.fit(data).transform(data)
        return res