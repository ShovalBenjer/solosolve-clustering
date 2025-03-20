"""
Preprocessing Pipeline for CFPB Complaint Processing System

This module implements the preprocessing pipeline for the CFPB complaint data,
including text preprocessing, feature engineering, and pipeline creation.
All processing is done without User Defined Functions (UDFs) for optimal performance.
"""

import logging
from pyspark.ml.feature import (
    StringIndexer, OneHotEncoder, VectorAssembler, 
    Tokenizer, StopWordsRemover, CountVectorizer, IDF,
    RegexTokenizer, NGram, Normalizer, SQLTransformer
)
from pyspark.ml import Pipeline, PipelineModel
from pyspark.sql.functions import (
    col, to_timestamp, datediff, when, lit, 
    length, expr, to_date, dayofweek, month, year, hour
)
from sparknlp.annotator import (
    Tokenizer as NLPTokenizer, 
    Normalizer as NLPNormalizer,
    LemmatizerModel, StopWordsCleaner,
    SentimentDLModel, BertSentenceEmbeddings,
    NerDLModel
)
from sparknlp.base import DocumentAssembler, Finisher

import src.config as config

logger = logging.getLogger(__name__)

def create_text_preprocessing_stages():
    """
    Create text preprocessing stages for the consumer complaint narrative
    using Spark NLP.
    
    Returns:
        list: List of preprocessing stages for the text pipeline
    """
    logger.info("Creating text preprocessing stages")
    
    # Initialize the document assembler to convert text to document
    document_assembler = DocumentAssembler() \
        .setInputCol("Consumer complaint narrative") \
        .setOutputCol("document")
    
    # Tokenize the text
    tokenizer = NLPTokenizer() \
        .setInputCols(["document"]) \
        .setOutputCol("tokens")
    
    # Clean and normalize tokens
    normalizer = NLPNormalizer() \
        .setInputCols(["tokens"]) \
        .setOutputCol("normalized_tokens") \
        .setLowercase(True)
    
    # Remove stop words
    stopwords_cleaner = StopWordsCleaner() \
        .setInputCols(["normalized_tokens"]) \
        .setOutputCol("cleaned_tokens") \
        .setCaseSensitive(False)
    
    # Lemmatize tokens to get base words
    lemmatizer = LemmatizerModel.pretrained() \
        .setInputCols(["cleaned_tokens"]) \
        .setOutputCol("lemmatized_tokens")
    
    # Convert tokens back to human-readable format
    finisher = Finisher() \
        .setInputCols(["lemmatized_tokens"]) \
        .setOutputCols(["processed_text"]) \
        .setOutputAsArray(True) \
        .setCleanAnnotations(False)
    
    # Convert token arrays to features
    cv = CountVectorizer() \
        .setInputCol("processed_text") \
        .setOutputCol("text_features") \
        .setVocabSize(10000) \
        .setMinDF(5)
    
    # Apply TF-IDF
    idf = IDF() \
        .setInputCol("text_features") \
        .setOutputCol("narrative_features")
    
    # Return all the stages
    return [
        document_assembler,
        tokenizer,
        normalizer,
        stopwords_cleaner,
        lemmatizer,
        finisher,
        cv,
        idf
    ]

def create_advanced_nlp_features():
    """
    Create advanced NLP features like sentiment analysis, named entity recognition,
    and BERT embeddings.
    
    Returns:
        list: List of stages for advanced NLP features
    """
    logger.info("Creating advanced NLP feature stages")
    
    # Document assembler for sentiment analysis
    document_assembler = DocumentAssembler() \
        .setInputCol("Consumer complaint narrative") \
        .setOutputCol("document_sentiment")
    
    # Sentiment analysis using pretrained model
    sentiment_model = SentimentDLModel.pretrained() \
        .setInputCols(["document_sentiment"]) \
        .setOutputCol("sentiment")
    
    # Finisher to get sentiment label
    sentiment_finisher = Finisher() \
        .setInputCols(["sentiment"]) \
        .setOutputCols(["sentiment_score"]) \
        .setCleanAnnotations(False)
    
    # Document assembler for BERT embeddings
    doc_assembler_bert = DocumentAssembler() \
        .setInputCol("Consumer complaint narrative") \
        .setOutputCol("document_bert")
    
    # BERT sentence embeddings
    bert_embeddings = BertSentenceEmbeddings.pretrained() \
        .setInputCols(["document_bert"]) \
        .setOutputCol("bert_embeddings")
    
    # Finisher to extract embeddings
    bert_finisher = Finisher() \
        .setInputCols(["bert_embeddings"]) \
        .setOutputCols(["complaint_embeddings"]) \
        .setCleanAnnotations(False)
    
    # Return all the stages
    return [
        document_assembler,
        sentiment_model,
        sentiment_finisher,
        doc_assembler_bert,
        bert_embeddings,
        bert_finisher
    ]

def create_date_feature_stages():
    """
    Create stages for date feature engineering without UDFs.
    
    Returns:
        list: List of SQLTransformer stages for date features
    """
    logger.info("Creating date feature stages")
    
    # Use SQLTransformer to perform date transformations
    date_features_sql = SQLTransformer().setStatement("""
        SELECT *, 
            dayofweek(DateReceivedTimestamp) as day_of_week_received,
            month(DateReceivedTimestamp) as month_received,
            year(DateReceivedTimestamp) as year_received,
            dayofweek(DateSentToCompanyTimestamp) as day_of_week_sent,
            month(DateSentToCompanyTimestamp) as month_sent,
            CASE
                WHEN dayofweek(DateReceivedTimestamp) IN (1, 7) THEN 1
                ELSE 0
            END as is_weekend_received
        FROM __THIS__
    """)
    
    return [date_features_sql]

def create_categorical_encoding_stages():
    """
    Create stages for encoding categorical features.
    
    Returns:
        list: List of stages for categorical encoding
    """
    logger.info("Creating categorical encoding stages")
    
    # Define categorical columns
    categorical_cols = [
        "Product", "Sub-product", "Issue", "Sub-issue", 
        "Company", "State", "Submitted via", 
        "Company response to consumer", "Consumer disputed?"
    ]
    
    stages = []
    indexed_cols = []
    encoded_cols = []
    
    # Create stages for each categorical column
    for column in categorical_cols:
        # Handle column names with spaces and special characters
        safe_col = column.replace(" ", "_").replace("-", "_").replace("?", "")
        index_col = f"{safe_col}_index"
        vector_col = f"{safe_col}_vec"
        
        # Create string indexer for the column
        indexer = StringIndexer() \
            .setInputCol(column) \
            .setOutputCol(index_col) \
            .setHandleInvalid("keep")
        
        # Create one-hot encoder for the indexed column
        encoder = OneHotEncoder() \
            .setInputCol(index_col) \
            .setOutputCol(vector_col)
        
        stages.append(indexer)
        stages.append(encoder)
        
        indexed_cols.append(index_col)
        encoded_cols.append(vector_col)
    
    return stages, encoded_cols

def create_feature_assembly_stage(categorical_cols, numerical_cols=None):
    """
    Create a vector assembler stage to combine all features.
    
    Args:
        categorical_cols (list): List of categorical feature columns
        numerical_cols (list, optional): List of numerical feature columns
    
    Returns:
        VectorAssembler: Stage for assembling features
    """
    logger.info("Creating feature assembly stage")
    
    # Combine all feature columns
    feature_cols = categorical_cols + ["narrative_features"]
    
    if numerical_cols:
        feature_cols.extend(numerical_cols)
    
    # Create vector assembler
    assembler = VectorAssembler() \
        .setInputCols(feature_cols) \
        .setOutputCol("features") \
        .setHandleInvalid("keep")
    
    return assembler

def create_derived_features_stage():
    """
    Create stages for derived features based on domain knowledge.
    
    Returns:
        list: List of SQLTransformer stages for derived features
    """
    logger.info("Creating derived features stage")
    
    # Use SQLTransformer to create derived features
    derived_features_sql = SQLTransformer().setStatement("""
        SELECT *, 
            -- Complaint complexity (word count)
            length(`Consumer complaint narrative`) - length(replace(`Consumer complaint narrative`, ' ', '')) + 1 as word_count,
            
            -- Time-based features
            CASE
                WHEN ProcessingTimeInDays <= 5 THEN 1
                ELSE 0
            END as quick_response,
            
            -- Consumer consent indicator
            CASE
                WHEN `Consumer consent provided?` = 'Consent provided' THEN 1
                ELSE 0
            END as has_consent,
            
            -- Derived categorical indicators
            CASE
                WHEN `Timely response?` = 'Yes' THEN 1
                ELSE 0
            END as is_timely_response
        FROM __THIS__
    """)
    
    return [derived_features_sql]

def create_preprocessing_pipeline():
    """
    Create the complete preprocessing pipeline by combining all stages.
    
    Returns:
        Pipeline: Complete preprocessing pipeline
    """
    logger.info("Creating complete preprocessing pipeline")
    
    # Collect all pipeline stages
    stages = []
    
    # Add text preprocessing stages
    stages.extend(create_text_preprocessing_stages())
    
    # Add advanced NLP features if enabled
    if config.ENABLE_ADVANCED_NLP:
        stages.extend(create_advanced_nlp_features())
    
    # Add derived features
    stages.extend(create_derived_features_stage())
    
    # Add date feature stages
    stages.extend(create_date_feature_stages())
    
    # Add categorical encoding stages
    cat_stages, encoded_cat_cols = create_categorical_encoding_stages()
    stages.extend(cat_stages)
    
    # Numerical columns to include in features
    numerical_cols = [
        "ProcessingTimeInDays", "word_count", "quick_response", 
        "has_consent", "is_timely_response",
        "day_of_week_received", "month_received", "is_weekend_received"
    ]
    
    # Add feature assembly stage
    stages.append(create_feature_assembly_stage(encoded_cat_cols, numerical_cols))
    
    # Create and return the pipeline
    return Pipeline(stages=stages)

def save_pipeline(pipeline_model, path=None):
    """
    Save the preprocessing pipeline model to disk.
    
    Args:
        pipeline_model (PipelineModel): Fitted pipeline model to save
        path (str, optional): Path to save the model. Defaults to None.
    
    Returns:
        str: Path where the pipeline was saved
    """
    save_path = path or config.PREPROCESSING_PIPELINE_PATH
    logger.info(f"Saving preprocessing pipeline to {save_path}")
    
    # Save the pipeline model
    pipeline_model.write().overwrite().save(str(save_path))
    
    return save_path

def load_pipeline(path=None):
    """
    Load a saved preprocessing pipeline model.
    
    Args:
        path (str, optional): Path to load the model from. Defaults to None.
    
    Returns:
        PipelineModel: Loaded pipeline model
    """
    load_path = path or config.PREPROCESSING_PIPELINE_PATH
    logger.info(f"Loading preprocessing pipeline from {load_path}")
    
    # Load and return the pipeline model
    return PipelineModel.load(str(load_path))

def validate_pipeline(pipeline_model, test_data):
    """
    Validate the preprocessing pipeline on test data.
    
    Args:
        pipeline_model (PipelineModel): Pipeline model to validate
        test_data (DataFrame): Test data to validate on
    
    Returns:
        DataFrame: Transformed test data
    """
    logger.info("Validating preprocessing pipeline")
    
    # Transform the test data
    try:
        transformed_data = pipeline_model.transform(test_data)
        
        # Verify that the features column exists and is not null
        feature_count = transformed_data.filter(col("features").isNull()).count()
        
        if feature_count > 0:
            logger.warning(f"Found {feature_count} null feature vectors in transformed data")
        else:
            logger.info("Validation successful: No null feature vectors found")
        
        return transformed_data
    
    except Exception as e:
        logger.error(f"Pipeline validation failed: {str(e)}")
        raise 