"""
Model Training Module

This module implements the core model training functionality for the CFPB Complaint Processing System.
It includes functions for data preparation, model training with cross-validation,
evaluation metrics calculation, and model persistence.
"""

import logging
from typing import Dict, Tuple, List

import mlflow
import numpy as np
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src import config
from src.logging_utils import setup_logging

# Set up logging
logger = setup_logging(log_level=config.LOG_LEVEL)

def prepare_training_data(data: DataFrame) -> Tuple[DataFrame, DataFrame, DataFrame]:
    """
    Prepare data splits for training, validation, and testing.
    
    Args:
        data (DataFrame): Preprocessed data
        
    Returns:
        Tuple[DataFrame, DataFrame, DataFrame]: Train, validation, and test sets
    """
    logger.info("Preparing data splits")
    
    # Create splits
    train_data, val_data, test_data = data.randomSplit([0.7, 0.15, 0.15], seed=42)
    
    # Cache the DataFrames for better performance
    train_data.cache()
    val_data.cache()
    test_data.cache()
    
    logger.info(f"Data split sizes - Train: {train_data.count()}, Val: {val_data.count()}, Test: {test_data.count()}")
    
    return train_data, val_data, test_data

def create_param_grid() -> Dict:
    """
    Create parameter grid for model tuning.
    
    Returns:
        Dict: Parameter grid for cross validation
    """
    lr = LogisticRegression(
        featuresCol="features",
        labelCol="label",
        maxIter=20
    )
    
    param_grid = ParamGridBuilder() \
        .addGrid(lr.regParam, [0.01, 0.1, 0.3]) \
        .addGrid(lr.elasticNetParam, [0.0, 0.5, 1.0]) \
        .build()
    
    return {
        "estimator": lr,
        "param_grid": param_grid
    }

def train_model(
    train_data: DataFrame,
    val_data: DataFrame,
    param_grid: Dict
) -> Tuple[LogisticRegression, Dict[str, float]]:
    """
    Train model using cross validation and hyperparameter tuning.
    
    Args:
        train_data (DataFrame): Training data
        val_data (DataFrame): Validation data
        param_grid (Dict): Parameter grid for tuning
        
    Returns:
        Tuple[LogisticRegression, Dict[str, float]]: Best model and validation metrics
    """
    with mlflow.start_run(run_name="complaint_classification"):
        # Set up cross validation
        evaluator = BinaryClassificationEvaluator(
            rawPredictionCol="rawPrediction",
            labelCol="label",
            metricName="areaUnderROC"
        )
        
        cv = CrossValidator(
            estimator=param_grid["estimator"],
            estimatorParamMaps=param_grid["param_grid"],
            evaluator=evaluator,
            numFolds=5,
            seed=42
        )
        
        # Fit cross validator
        logger.info("Starting cross validation")
        cv_model = cv.fit(train_data)
        best_model = cv_model.bestModel
        
        # Log parameters
        mlflow.log_param("regParam", best_model._java_obj.getRegParam())
        mlflow.log_param("elasticNetParam", best_model._java_obj.getElasticNetParam())
        
        # Evaluate on validation set
        val_predictions = best_model.transform(val_data)
        val_metrics = evaluate_model(val_predictions)
        
        # Log metrics
        for metric, value in val_metrics.items():
            mlflow.log_metric(f"val_{metric}", value)
        
        return best_model, val_metrics

def evaluate_model(predictions: DataFrame) -> Dict[str, float]:
    """
    Calculate evaluation metrics for model predictions.
    
    Args:
        predictions (DataFrame): Model predictions
        
    Returns:
        Dict[str, float]: Dictionary of evaluation metrics
    """
    # Binary classification metrics
    binary_evaluator = BinaryClassificationEvaluator(labelCol="label")
    
    # Multi-class metrics
    multi_evaluator = MulticlassClassificationEvaluator(labelCol="label")
    
    metrics = {
        "areaUnderROC": binary_evaluator.setMetricName("areaUnderROC").evaluate(predictions),
        "areaUnderPR": binary_evaluator.setMetricName("areaUnderPR").evaluate(predictions),
        "accuracy": multi_evaluator.setMetricName("accuracy").evaluate(predictions),
        "f1": multi_evaluator.setMetricName("f1").evaluate(predictions),
        "precision": multi_evaluator.setMetricName("weightedPrecision").evaluate(predictions),
        "recall": multi_evaluator.setMetricName("weightedRecall").evaluate(predictions)
    }
    
    return metrics

def analyze_feature_importance(
    model: LogisticRegression,
    feature_names: List[str]
) -> DataFrame:
    """
    Analyze and log feature importance from the trained model.
    
    Args:
        model (LogisticRegression): Trained model
        feature_names (List[str]): List of feature names
        
    Returns:
        DataFrame: Feature importance analysis
    """
    # Get feature coefficients
    coefficients = model.coefficients.toArray()
    
    # Create feature importance DataFrame
    feature_importance = [(name, abs(coef)) for name, coef in zip(feature_names, coefficients)]
    feature_importance.sort(key=lambda x: x[1], reverse=True)
    
    # Log top features
    logger.info("Top 10 most important features:")
    for name, importance in feature_importance[:10]:
        logger.info(f"{name}: {importance:.4f}")
        mlflow.log_metric(f"feature_importance_{name}", importance)
    
    return feature_importance

def save_model(model: LogisticRegression, metrics: Dict[str, float]) -> str:
    """
    Save the trained model and its metrics.
    
    Args:
        model (LogisticRegression): Trained model
        metrics (Dict[str, float]): Model evaluation metrics
        
    Returns:
        str: Path where model is saved
    """
    save_path = str(config.MODELS_DIR / "complaint_classifier")
    
    # Save model
    model.save(save_path)
    
    # Log model and metrics with MLflow
    mlflow.spark.log_model(model, "complaint_classifier")
    for metric, value in metrics.items():
        mlflow.log_metric(metric, value)
    
    return save_path 