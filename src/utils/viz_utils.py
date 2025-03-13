"""
src/utils/viz_utils.py - Visualization utilities for CFPB complaint clusters

This module provides utilities for visualizing and analyzing CFPB complaint clusters.
It includes functions for dimensionality reduction, cluster visualization, and
generating insights from clustering results.

Importance:
    - Enables visual interpretation of high-dimensional embedding spaces
    - Provides tools for analyzing cluster characteristics
    - Helps identify patterns and trends in complaint data

Main Functions:
    - plot_clusters: Visualizes clusters in 2D or 3D space
    - reduce_dimensions: Reduces high-dimensional embeddings to 2D/3D
    - extract_cluster_keywords: Extracts representative keywords for each cluster
    - plot_cluster_distribution: Visualizes the distribution of complaints across clusters
    - generate_cluster_report: Generates a comprehensive report on cluster characteristics

Mathematical Aspects:
    - Implements dimensionality reduction techniques (t-SNE, UMAP, PCA)
    - Applies TF-IDF for keyword extraction
    - Uses silhouette analysis for cluster quality assessment

Time Complexity:
    - Dimensionality reduction: O(n²) for t-SNE, O(n log n) for UMAP
    - Keyword extraction: O(n*v) where v is vocabulary size
    - Visualization: O(n) for most plotting functions

Space Complexity:
    - O(n*d) for storing reduced dimensions
    - O(n*k) for cluster analysis where k is number of clusters

Dependencies:
    - matplotlib, seaborn: For visualization
    - sklearn: For dimensionality reduction and metrics
    - umap-learn: For UMAP dimensionality reduction
    - plotly: For interactive visualizations
"""

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from typing import Dict, List, Tuple, Optional, Union, Any
import plotly.express as px
import plotly.graph_objects as go
from sklearn.manifold import TSNE
from sklearn.decomposition import PCA
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics import silhouette_score
import umap
import io
import base64
from wordcloud import WordCloud
from collections import Counter
import os
import json

# Set default style
sns.set(style="whitegrid")
plt.rcParams["figure.figsize"] = (12, 8)

def reduce_dimensions(
    embeddings: np.ndarray,
    method: str = "tsne",
    n_components: int = 2,
    random_state: int = 42,
    **kwargs
) -> np.ndarray:
    """
    Reduces high-dimensional embeddings to 2D or 3D for visualization.
    
    This function applies dimensionality reduction techniques to transform
    high-dimensional embeddings into a lower-dimensional space for visualization.
    
    Parameters:
    -----------
    embeddings : np.ndarray
        High-dimensional embeddings array
    method : str
        Dimensionality reduction method: 'tsne', 'umap', or 'pca'
    n_components : int
        Number of dimensions in the output (2 or 3)
    random_state : int
        Random seed for reproducibility
    **kwargs : dict
        Additional parameters for the specific reduction method
        
    Returns:
    --------
    np.ndarray
        Reduced embeddings in 2D or 3D
        
    Notes:
    ------
    - t-SNE is good for preserving local structure but slow for large datasets
    - UMAP is faster and preserves both local and global structure
    - PCA is fastest but preserves less structure
    """
    if method == "tsne":
        # t-SNE parameters
        perplexity = kwargs.get("perplexity", 30)
        learning_rate = kwargs.get("learning_rate", 200)
        n_iter = kwargs.get("n_iter", 1000)
        
        reducer = TSNE(
            n_components=n_components,
            perplexity=perplexity,
            learning_rate=learning_rate,
            n_iter=n_iter,
            random_state=random_state
        )
    
    elif method == "umap":
        # UMAP parameters
        n_neighbors = kwargs.get("n_neighbors", 15)
        min_dist = kwargs.get("min_dist", 0.1)
        
        reducer = umap.UMAP(
            n_components=n_components,
            n_neighbors=n_neighbors,
            min_dist=min_dist,
            random_state=random_state
        )
    
    elif method == "pca":
        # PCA parameters
        reducer = PCA(n_components=n_components, random_state=random_state)
    
    else:
        raise ValueError(f"Unknown dimensionality reduction method: {method}")
    
    # Apply reduction
    reduced_embeddings = reducer.fit_transform(embeddings)
    
    return reduced_embeddings

def plot_clusters(
    reduced_embeddings: np.ndarray,
    cluster_labels: np.ndarray,
    title: str = "Complaint Clusters",
    interactive: bool = True,
    hover_data: Optional[pd.DataFrame] = None,
    save_path: Optional[str] = None
) -> Union[None, str]:
    """
    Visualizes clusters in 2D or 3D space.
    
    This function creates a scatter plot of the reduced embeddings, colored by
    cluster label, to visualize the clustering results.
    
    Parameters:
    -----------
    reduced_embeddings : np.ndarray
        Reduced embeddings in 2D or 3D
    cluster_labels : np.ndarray
        Cluster labels for each point
    title : str
        Plot title
    interactive : bool
        Whether to create an interactive Plotly visualization
    hover_data : pd.DataFrame, optional
        Additional data to show on hover
    save_path : str, optional
        Path to save the visualization
        
    Returns:
    --------
    Union[None, str]
        If interactive=True, returns HTML string of the plot
        
    Notes:
    ------
    - Creates 2D or 3D visualization based on reduced_embeddings shape
    - Interactive plots include hover information
    - Static plots are saved to disk if save_path is provided
    """
    # Determine if 2D or 3D
    is_3d = reduced_embeddings.shape[1] == 3
    
    # Create DataFrame for plotting
    plot_df = pd.DataFrame(
        reduced_embeddings,
        columns=["x", "y"] if not is_3d else ["x", "y", "z"]
    )
    
    # Add cluster labels
    plot_df["cluster"] = cluster_labels
    
    # Convert cluster labels to strings
    plot_df["cluster"] = plot_df["cluster"].astype(str)
    
    # Add hover data if provided
    if hover_data is not None:
        for col in hover_data.columns:
            plot_df[col] = hover_data[col].values
    
    if interactive:
        # Create interactive Plotly visualization
        if is_3d:
            fig = px.scatter_3d(
                plot_df,
                x="x",
                y="y",
                z="z",
                color="cluster",
                title=title,
                opacity=0.7,
                hover_data=hover_data.columns if hover_data is not None else None
            )
        else:
            fig = px.scatter(
                plot_df,
                x="x",
                y="y",
                color="cluster",
                title=title,
                opacity=0.7,
                hover_data=hover_data.columns if hover_data is not None else None
            )
        
        # Update layout
        fig.update_layout(
            template="plotly_white",
            legend_title_text="Cluster",
            width=1000,
            height=800
        )
        
        # Save if requested
        if save_path:
            fig.write_html(save_path)
        
        # Return HTML
        return fig.to_html(include_plotlyjs="cdn")
    
    else:
        # Create static Matplotlib visualization
        plt.figure(figsize=(12, 10))
        
        if is_3d:
            ax = plt.axes(projection="3d")
            scatter = ax.scatter(
                plot_df["x"],
                plot_df["y"],
                plot_df["z"],
                c=plot_df["cluster"].astype("category").cat.codes,
                cmap="tab20",
                alpha=0.7,
                s=50
            )
            ax.set_xlabel("Dimension 1")
            ax.set_ylabel("Dimension 2")
            ax.set_zlabel("Dimension 3")
        else:
            scatter = plt.scatter(
                plot_df["x"],
                plot_df["y"],
                c=plot_df["cluster"].astype("category").cat.codes,
                cmap="tab20",
                alpha=0.7,
                s=50
            )
            plt.xlabel("Dimension 1")
            plt.ylabel("Dimension 2")
        
        # Add legend
        legend1 = plt.legend(
            *scatter.legend_elements(),
            title="Clusters",
            loc="upper right"
        )
        plt.gca().add_artist(legend1)
        
        # Add title
        plt.title(title, fontsize=16)
        
        # Save if requested
        if save_path:
            plt.savefig(save_path, dpi=300, bbox_inches="tight")
            plt.close()
        else:
            plt.tight_layout()
            plt.show()
        
        return None

def extract_cluster_keywords(
    texts: List[str],
    cluster_labels: np.ndarray,
    n_keywords: int = 10,
    min_df: float = 0.01,
    max_df: float = 0.9
) -> Dict[str, List[str]]:
    """
    Extracts representative keywords for each cluster.
    
    This function uses TF-IDF to identify the most representative keywords
    for each cluster based on the complaint narratives.
    
    Parameters:
    -----------
    texts : List[str]
        List of complaint narratives
    cluster_labels : np.ndarray
        Cluster labels for each complaint
    n_keywords : int
        Number of keywords to extract per cluster
    min_df : float
        Minimum document frequency for TF-IDF
    max_df : float
        Maximum document frequency for TF-IDF
        
    Returns:
    --------
    Dict[str, List[str]]
        Dictionary mapping cluster labels to lists of keywords
        
    Notes:
    ------
    - Uses TF-IDF to identify distinctive terms in each cluster
    - Filters out common and rare terms using min_df and max_df
    - Returns the top n_keywords for each cluster
    """
    # Create DataFrame
    df = pd.DataFrame({"text": texts, "cluster": cluster_labels})
    
    # Initialize TF-IDF vectorizer
    vectorizer = TfidfVectorizer(
        max_features=10000,
        min_df=min_df,
        max_df=max_df,
        stop_words="english"
    )
    
    # Get unique clusters
    unique_clusters = sorted(df["cluster"].unique())
    
    # Initialize results dictionary
    cluster_keywords = {}
    
    # Process each cluster
    for cluster in unique_clusters:
        # Skip noise cluster (-1)
        if cluster == -1:
            continue
        
        # Get texts for this cluster
        cluster_texts = df[df["cluster"] == cluster]["text"].tolist()
        
        # Skip if empty
        if not cluster_texts:
            continue
        
        # Fit vectorizer
        X = vectorizer.fit_transform(cluster_texts)
        
        # Get feature names
        feature_names = vectorizer.get_feature_names_out()
        
        # Calculate average TF-IDF score for each term
        avg_tfidf = X.mean(axis=0).A1
        
        # Get top terms
        top_indices = avg_tfidf.argsort()[-n_keywords:][::-1]
        top_terms = [feature_names[i] for i in top_indices]
        
        # Store results
        cluster_keywords[str(cluster)] = top_terms
    
    return cluster_keywords

def plot_cluster_distribution(
    cluster_labels: np.ndarray,
    title: str = "Complaint Distribution Across Clusters",
    save_path: Optional[str] = None
) -> None:
    """
    Visualizes the distribution of complaints across clusters.
    
    This function creates a bar chart showing the number of complaints in each
    cluster to visualize the cluster size distribution.
    
    Parameters:
    -----------
    cluster_labels : np.ndarray
        Cluster labels for each complaint
    title : str
        Plot title
    save_path : str, optional
        Path to save the visualization
        
    Notes:
    ------
    - Creates a bar chart of cluster sizes
    - Highlights the noise cluster (-1) if present
    - Saves the plot to disk if save_path is provided
    """
    # Count complaints per cluster
    cluster_counts = Counter(cluster_labels)
    
    # Convert to DataFrame
    df = pd.DataFrame({
        "cluster": list(cluster_counts.keys()),
        "count": list(cluster_counts.values())
    })
    
    # Sort by cluster label
    df = df.sort_values("cluster")
    
    # Create plot
    plt.figure(figsize=(12, 6))
    
    # Create bar colors (highlight noise cluster)
    colors = ["#ff7f0e" if c == -1 else "#1f77b4" for c in df["cluster"]]
    
    # Plot bars
    bars = plt.bar(df["cluster"].astype(str), df["count"], color=colors)
    
    # Add count labels
    for bar in bars:
        height = bar.get_height()
        plt.text(
            bar.get_x() + bar.get_width()/2.,
            height + 5,
            f"{int(height)}",
            ha="center",
            va="bottom",
            fontsize=10
        )
    
    # Add labels and title
    plt.xlabel("Cluster", fontsize=12)
    plt.ylabel("Number of Complaints", fontsize=12)
    plt.title(title, fontsize=16)
    
    # Add legend if noise cluster exists
    if -1 in cluster_counts:
        plt.legend(["Regular Clusters", "Noise"], loc="upper right")
    
    # Adjust layout
    plt.tight_layout()
    
    # Save if requested
    if save_path:
        plt.savefig(save_path, dpi=300, bbox_inches="tight")
        plt.close()
    else:
        plt.show()

def generate_wordcloud(
    texts: List[str],
    max_words: int = 100,
    width: int = 800,
    height: int = 400,
    background_color: str = "white"
) -> plt.Figure:
    """
    Generates a word cloud from complaint texts.
    
    This function creates a word cloud visualization from the provided texts,
    highlighting the most frequent terms.
    
    Parameters:
    -----------
    texts : List[str]
        List of texts to generate word cloud from
    max_words : int
        Maximum number of words to include
    width : int
        Width of the word cloud image
    height : int
        Height of the word cloud image
    background_color : str
        Background color of the word cloud
        
    Returns:
    --------
    plt.Figure
        Matplotlib figure containing the word cloud
        
    Notes:
    ------
    - Combines all texts into a single string
    - Filters out common stop words
    - Sizes words based on frequency
    """
    # Combine texts
    text = " ".join(texts)
    
    # Generate word cloud
    wordcloud = WordCloud(
        max_words=max_words,
        width=width,
        height=height,
        background_color=background_color,
        colormap="viridis",
        stopwords="english",
        random_state=42
    ).generate(text)
    
    # Create figure
    fig, ax = plt.subplots(figsize=(width/100, height/100))
    ax.imshow(wordcloud, interpolation="bilinear")
    ax.axis("off")
    
    return fig

def calculate_cluster_metrics(
    embeddings: np.ndarray,
    cluster_labels: np.ndarray
) -> Dict[str, float]:
    """
    Calculates quality metrics for clustering results.
    
    This function computes various metrics to evaluate the quality of the
    clustering results, including silhouette score and inertia.
    
    Parameters:
    -----------
    embeddings : np.ndarray
        Embeddings used for clustering
    cluster_labels : np.ndarray
        Cluster labels for each point
        
    Returns:
    --------
    Dict[str, float]
        Dictionary of metric names and values
        
    Notes:
    ------
    - Silhouette score measures how similar points are to their own cluster
      compared to other clusters (higher is better)
    - Only calculates metrics if there are at least 2 clusters
    """
    metrics = {}
    
    # Get unique clusters (excluding noise)
    unique_clusters = np.unique(cluster_labels)
    valid_clusters = [c for c in unique_clusters if c != -1]
    
    # Only calculate if we have at least 2 clusters
    if len(valid_clusters) >= 2:
        # Get indices of points in valid clusters
        valid_indices = np.where(np.isin(cluster_labels, valid_clusters))[0]
        
        # Skip if not enough points
        if len(valid_indices) > 1:
            # Get valid embeddings and labels
            valid_embeddings = embeddings[valid_indices]
            valid_labels = cluster_labels[valid_indices]
            
            # Calculate silhouette score
            try:
                silhouette = silhouette_score(valid_embeddings, valid_labels)
                metrics["silhouette_score"] = silhouette
            except:
                metrics["silhouette_score"] = float("nan")
    
    # Count points per cluster
    cluster_counts = Counter(cluster_labels)
    metrics["num_clusters"] = len([c for c in cluster_counts.keys() if c != -1])
    metrics["noise_points"] = cluster_counts.get(-1, 0)
    metrics["total_points"] = len(cluster_labels)
    
    return metrics

def generate_cluster_report(
    texts: List[str],
    embeddings: np.ndarray,
    cluster_labels: np.ndarray,
    metadata: Optional[pd.DataFrame] = None,
    n_keywords: int = 10,
    output_path: Optional[str] = None
) -> Dict[str, Any]:
    """
    Generates a comprehensive report on cluster characteristics.
    
    This function creates a detailed report on the clustering results,
    including metrics, visualizations, and insights.
    
    Parameters:
    -----------
    texts : List[str]
        List of complaint narratives
    embeddings : np.ndarray
        Embeddings used for clustering
    cluster_labels : np.ndarray
        Cluster labels for each point
    metadata : pd.DataFrame, optional
        Additional metadata for each complaint
    n_keywords : int
        Number of keywords to extract per cluster
    output_path : str, optional
        Path to save the report
        
    Returns:
    --------
    Dict[str, Any]
        Dictionary containing report components
        
    Notes:
    ------
    - Calculates cluster metrics
    - Extracts representative keywords for each cluster
    - Generates visualizations of clusters
    - Saves report to disk if output_path is provided
    """
    # Initialize report dictionary
    report = {}
    
    # Calculate metrics
    report["metrics"] = calculate_cluster_metrics(embeddings, cluster_labels)
    
    # Extract keywords
    report["keywords"] = extract_cluster_keywords(texts, cluster_labels, n_keywords=n_keywords)
    
    # Reduce dimensions for visualization
    reduced_embeddings = reduce_dimensions(embeddings, method="umap", n_components=2)
    
    # Create visualization
    if metadata is not None:
        hover_data = metadata.copy()
    else:
        hover_data = pd.DataFrame({"text": texts})
    
    # Generate cluster visualization
    if output_path:
        viz_path = f"{output_path}/cluster_visualization.html"
        os.makedirs(os.path.dirname(viz_path), exist_ok=True)
    else:
        viz_path = None
    
    report["visualization"] = plot_clusters(
        reduced_embeddings,
        cluster_labels,
        hover_data=hover_data,
        interactive=True,
        save_path=viz_path
    )
    
    # Generate cluster distribution
    if output_path:
        dist_path = f"{output_path}/cluster_distribution.png"
    else:
        dist_path = None
    
    plot_cluster_distribution(cluster_labels, save_path=dist_path)
    
    # Generate summary statistics
    if metadata is not None:
        # Add cluster labels to metadata
        metadata_with_clusters = metadata.copy()
        metadata_with_clusters["cluster"] = cluster_labels
        
        # Group by cluster
        report["cluster_stats"] = metadata_with_clusters.groupby("cluster").agg({
            col: ["count", "mean", "std", "min", "max"] 
            for col in metadata.select_dtypes(include=np.number).columns
        })
    
    # Save report if requested
    if output_path:
        # Create output directory
        os.makedirs(output_path, exist_ok=True)
        
        # Save metrics and keywords
        with open(f"{output_path}/cluster_metrics.json", "w") as f:
            json.dump(report["metrics"], f, indent=2)
        
        with open(f"{output_path}/cluster_keywords.json", "w") as f:
            json.dump(report["keywords"], f, indent=2)
        
        # Save visualization HTML
        with open(f"{output_path}/cluster_visualization.html", "w") as f:
            f.write(report["visualization"])
        
        # Save cluster stats if available
        if "cluster_stats" in report:
            report["cluster_stats"].to_csv(f"{output_path}/cluster_stats.csv")
    
    return report 