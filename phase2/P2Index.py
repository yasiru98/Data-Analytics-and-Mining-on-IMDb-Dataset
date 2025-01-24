import psycopg2
import time

connection = psycopg2.connect(
    dbname="project",
    user="postgres",
    password="admin",
    host="localhost",
    port="5432"
)

def execute_query(query, query_name):
    try:
        with connection.cursor() as cursor:
            print(f"\nExecuting {query_name}...\n")
            start_time = time.time()
            cursor.execute(query)
            rows = cursor.fetchall()
            end_time = time.time()
            execution_time = end_time - start_time
            print(f"Execution time: {execution_time:.4f} seconds\n")
            for row in rows[:5]:
                print(row)
            print("\n" + "-" * 50 + "\n")
            return execution_time
    except Exception as e:
        print(f"An error occurred while executing {query_name}: {e}")

# SQL queries
queries = {
    "Query 1: Top 5 Artists with the Most Genre Diversity in Their Titles": """
    SELECT A.primaryName, COUNT(DISTINCT G.genreName) AS genre_diversity
    FROM Artist A
    JOIN Principals P ON A.nconst = P.nconst
    JOIN Title_Genre TG ON P.tconst = TG.tconst
    JOIN Genre G ON TG.GenreID = G.GenreID
    GROUP BY A.primaryName
    ORDER BY genre_diversity DESC
    LIMIT 5;
    """,
    "Query 2: Average Rating per Genre": """
    SELECT G.genreName, AVG(R.averageRating) AS avg_rating
    FROM Genre G
    JOIN Title_Genre TG ON G.GenreID = TG.GenreID
    JOIN Rating R ON TG.tconst = R.tconst
    GROUP BY G.genreName
    ORDER BY avg_rating DESC;
    """,
    "Query 3: Artists with the Longest Career Span in Media": """
    SELECT A.primaryName, MIN(T.startYear) AS career_start, MAX(T.endYear) AS career_end, 
           (MAX(T.endYear) - MIN(T.startYear)) AS career_span
    FROM Artist A
    JOIN Principals P ON A.nconst = P.nconst
    JOIN Title T ON P.tconst = T.tconst
    WHERE T.startYear IS NOT NULL AND T.endYear IS NOT NULL
    GROUP BY A.primaryName
    ORDER BY career_span DESC
    LIMIT 5;
    """,
    "Query 4: Most Frequent Collaborations Between Artists": """
    SELECT A1.primaryName AS artist_1, A2.primaryName AS artist_2, COUNT(*) AS collaboration_count
    FROM Principals P1
    JOIN Principals P2 ON P1.tconst = P2.tconst AND P1.nconst < P2.nconst
    JOIN Artist A1 ON P1.nconst = A1.nconst
    JOIN Artist A2 ON P2.nconst = A2.nconst
    GROUP BY A1.primaryName, A2.primaryName
    ORDER BY collaboration_count DESC
    LIMIT 5;
    """,
    "Query 5: Average Runtime of Titles by Genre and Year": """
    SELECT G.genreName, T.startYear, AVG(T.runtimeMinutes) AS avg_runtime
    FROM Title T
    JOIN Title_Genre TG ON T.tconst = TG.tconst
    JOIN Genre G ON TG.GenreID = G.GenreID
    WHERE T.startYear IS NOT NULL AND T.runtimeMinutes IS NOT NULL
    GROUP BY G.genreName, T.startYear
    ORDER BY G.genreName, T.startYear;
    """
}

# Index definitions
indexes = [
    "CREATE INDEX idx_artist_nconst ON Artist(nconst);",
    "CREATE INDEX idx_principals_nconst ON Principals(nconst);",
    "CREATE INDEX idx_principals_tconst ON Principals(tconst);",
    "CREATE INDEX idx_title_tconst ON Title(tconst);",
    "CREATE INDEX idx_genre_genre_id ON Genre(GenreID);",
    "CREATE INDEX idx_rating_tconst ON Rating(tconst);",
    # Composite index: Optimizing joins involving (tconst, nconst)
    "CREATE INDEX idx_principals_tconst_nconst ON Principals(tconst, nconst);",
    # Partial index: Optimizing queries filtering by high ratings
    "CREATE INDEX idx_high_average_rating ON Rating(averageRating) WHERE averageRating > 8.0;"
]
# Measure execution times before indexing
print("Execution times without indexes:")
execution_times_without_indexes = {}
for query_name, query in queries.items():
    execution_times_without_indexes[query_name] = execute_query(query, query_name)
# Create indexes
with connection.cursor() as cursor:
    print("\nCreating indexes...\n")
    for index_query in indexes:
        cursor.execute(index_query)
    connection.commit()
    print("Indexes created successfully.\n")
# Measure execution times after indexing
print("Execution times with indexes:")
execution_times_with_indexes = {}
for query_name, query in queries.items():
    execution_times_with_indexes[query_name] = execute_query(query, query_name)
# Compare performance
print("\nComparison of Execution Times:")
for query_name in queries.keys():
    without_index = execution_times_without_indexes[query_name]
    with_index = execution_times_with_indexes[query_name]
    print(f"{query_name}:")
    print(f" - Without indexes: {without_index:.4f} seconds")
    print(f" - With indexes: {with_index:.4f} seconds")
    if with_index < without_index:
        improvement = without_index - with_index
        print(f" - Improvement: {improvement:.4f} seconds ({(improvement / without_index) * 100:.2f}% faster)")
    else:
        print(" - Indexing had no significant impact or slightly increased execution time.")
    print("-" * 50)
connection.close()