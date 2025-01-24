import pymongo
import csv
import time

# Increase the field size limit to handle large fields
csv.field_size_limit(10**7)
"""
CSCI-620: Project Phase 2

This program is used to load the data into the document database for Project 
Phase 2

"""

# Connect to MongoDB
client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client["movie_dataset"]

# Load Artists into MongoDB
def load_artists(tsv_file):
    collection = db["artists"]
    start_time = time.time()
    with open(tsv_file, 'r', encoding='utf-8') as f:
        reader = csv.reader(f, delimiter='\t')
        next(reader)  # Skip the header
        for row_num, row in enumerate(reader, start=1):
            document = {
                "nconst": row[0],
                "primaryName": row[1],
                "birthYear": int(row[2]) if row[2] != "\\N" else None,
                "deathYear": int(row[3]) if row[3] != "\\N" else None,
                "primaryProfession": row[4].split(",") if row[4] != "\\N" else [],
                "knownForTitles": row[5].split(",") if row[5] != "\\N" else []
            }
            collection.insert_one(document)
    elapsed_minutes = (time.time() - start_time) / 60
    print(f"Completed loading artists. Time taken: {elapsed_minutes:.2f} minutes.")

# Load Titles into MongoDB
def load_titles(tsv_file, ratings_file, akas_file):
    collection = db["titles"]
    # Load Ratings into a Dictionary
    ratings = {}
    with open(ratings_file, 'r', encoding='utf-8') as f:
        reader = csv.reader(f, delimiter='\t')
        next(reader)
        for row in reader:
            ratings[row[0]] = {"averageRating": float(row[1]), "numVotes": int(row[2])}

    # Load Akas file data into a Dictionary
    akas = {}
    with open(akas_file, 'r', encoding='utf-8') as f:
        reader = csv.reader(f, delimiter='\t')
        next(reader)
        for row in reader:
            tconst = row[0]
            aka = {
                "ordering": int(row[1]),
                "title": row[2],
                "region": row[3] if row[3] != "\\N" else None,
                "language": row[4] if row[4] != "\\N" else None,
                "types": row[5] if row[5] != "\\N" else None,
                "attributes": row[6] if row[6] != "\\N" else None,
                "isOriginalTitle": bool(int(row[7])) if row[7] != "\\N" else False
            }
            if tconst not in akas:
                akas[tconst] = []
            akas[tconst].append(aka)

    # Load Titles into MongoDB
    start_time = time.time()
    with open(tsv_file, 'r', encoding='utf-8') as f:
        reader = csv.reader(f, delimiter='\t')
        next(reader)
        for row_num, row in enumerate(reader, start=1):
            # Ensure the row has at least 9 elements to account for data
            # inconsistencies
            if len(row) < 9:
                print(f"Skipping row {row_num}: Row has fewer than 9 columns.")
                continue
            document = {
                "tconst": row[0],
                "titleType": row[1],
                "primaryTitle": row[2],
                "originalTitle": row[3],
                "isAdult": bool(int(row[4])) if row[4] != "\\N" else False,
                "startYear": int(row[5]) if row[5] != "\\N" else None,
                "endYear": int(row[6]) if row[6] != "\\N" else None,
                "runtimeMinutes": int(row[7]) if row[7] != "\\N" else None,
                "genres": row[8].split(",") if row[8] != "\\N" else [],
                "averageRating": ratings.get(row[0], {}).get("averageRating"),
                "numVotes": ratings.get(row[0], {}).get("numVotes"),
                "localizations": akas.get(row[0], [])
            }
            collection.insert_one(document)
    elapsed_minutes = (time.time() - start_time) / 60
    print(f"Completed loading titles. Time taken: {elapsed_minutes:.2f} minutes.")

# Load Principals into MongoDB
def load_principals(tsv_file):
    collection = db["principals"]
    start_time = time.time()
    with open(tsv_file, 'r', encoding='utf-8') as f:
        reader = csv.reader(f, delimiter='\t')
        next(reader)
        for row_num, row in enumerate(reader, start=1):
            document = {
                "tconst": row[0],
                "ordering": int(row[1]),
                "nconst": row[2],
                "category": row[3],
                "job": row[4] if row[4] != "\\N" else None,
                "characters": row[5].strip("[]").split(",") if row[5] != "\\N" else []
            }
            collection.insert_one(document)
    elapsed_minutes = (time.time() - start_time) / 60
    print(f"Completed loading principals. Time taken: {elapsed_minutes:.2f} minutes.")

# Load Collections
load_artists('data/name.basics.tsv')
load_titles('data/title.basics.tsv', 'data/title.ratings.tsv', 'data/title.akas.tsv')
load_principals('data/title.principals.tsv')
