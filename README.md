# Data-Analytics-and-Mining-on-IMDb-Dataset
This project leverages Big Data techniques to analyze the IMDb dataset, uncovering patterns in movie genres, artist professions, and audience ratings. It combines relational and document-oriented database models, implements data cleaning and transformation pipelines, and applies frequent itemset mining and association rule mining.
Key Features:

    Hybrid Database Design:
        PostgreSQL for relational modeling with advanced indexing for optimized query performance.
        MongoDB for schema-less, hierarchical storage with embedded data for efficient read-heavy operations.

    Data Cleaning:
    Python and Pandas pipelines to standardize, transform, and handle multi-valued fields, ensuring high-quality data for analysis.

    Frequent Itemset and Association Rule Mining:
    Applied efficient_apriori and MLxtend to uncover frequent co-occurrences of genres and professions, and generate actionable rules like "Action → Adventure."

    Performance Optimization:
    Indexed relational queries achieving significant speed improvements and reduced redundancy in MongoDB using nested structures.

    Visualization:
    Insights visualized using Matplotlib and Seaborn, highlighting trends like popular genre pairings and frequent profession collaborations.

Tools Used:

    Databases: PostgreSQL, MongoDB
    Programming: Python, Pandas
    Mining Libraries: efficient_apriori, MLxtend
    Visualization: Matplotlib, Seaborn

This project demonstrates advanced database management, data cleaning, and mining techniques, providing actionable insights for recommendation systems, market analysis, and trend prediction in the film industry.
