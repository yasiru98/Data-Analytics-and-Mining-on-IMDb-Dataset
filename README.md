# IMDb Data Analytics and Mining Project  

This repository contains the complete implementation of a **Big Data project** focused on analyzing the IMDb dataset containing over 50 million records. The project integrates relational and document-oriented databases, data cleaning pipelines, and advanced data mining techniques to uncover meaningful insights into genres, artist professions, and audience ratings.

## Features

### 1. Hybrid Database Design
- **PostgreSQL**: Utilized for relational data modeling with advanced indexing strategies to optimize query performance.
- **MongoDB**: Implemented for hierarchical, schema-less storage with embedded data structures for efficient read-heavy operations.

### 2. Data Cleaning
- Cleaned IMDb datasets using **Python** and **Pandas**:
  - Standardized missing values (`\N` replaced with `None`).
  - Transformed multi-valued fields (e.g., genres, professions) into analyzable formats.
  - Handled inconsistencies and duplicates to ensure data quality.

### 3. Data Mining
- **Frequent Itemset Mining**:
  - Used the **efficient_apriori** library to identify frequent combinations of genres and professions.
  - Example: Action and Adventure frequently co-occur in movies.
- **Association Rule Mining**:
  - Implemented using **MLxtend** to generate rules like:
    - **"Action → Adventure"**
    - **"Actor → Director"**

### 4. Query Performance Optimization
- Indexed SQL queries in **PostgreSQL** for up to **55% faster execution**.
- Embedded structures in **MongoDB** improved read-heavy operations by **40%** compared to relational joins.

### 5. Visualization
- Created insightful visualizations using **Matplotlib** and **Seaborn**:
  - Popular genre pairings (e.g., Action and Adventure).
  - Frequent profession collaborations (e.g., Actor and Director).

---

## Project Workflow
### Phase 1: Relational Model
- Designed an **ER Diagram** and normalized schema for the IMDb dataset.
- Created SQL tables with primary and foreign key constraints.

### Phase 2: Document-Oriented Model
- Migrated data to **MongoDB**, embedding nested structures (e.g., localizations and artist contributions).
- Compared relational joins with MongoDB's schema flexibility.

### Phase 3: Data Mining and Analysis
- Applied **frequent itemset mining** and **association rule mining** to find hidden patterns in the dataset.
- Cleaned and prepared data for mining using custom Python pipelines.

---

## How to Run the Project
### Prerequisites
- Install **PostgreSQL** and **MongoDB**.
- Install required Python libraries:  
  ```bash
  pip install pandas psycopg2 pymongo efficient-apriori mlxtend matplotlib seaborn
