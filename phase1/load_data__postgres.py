import psycopg2
import csv
import time

"""
CSCI-620: Project Phase 1

This program is used to load the data into the database for Project Phase 1

"""

# Database connection
connection = psycopg2.connect(
    dbname="project",
    user="postgres",
    password="admin",
    host="localhost",
    port="5432"
)
cursor = connection.cursor()


# Function to insert data from TSV files
def insert_data_from_tsv(table_name, tsv_file, query, process_row_func):
    start_time = time.time()
    with open(tsv_file, 'r', encoding='utf-8') as f:  # Read plain TSV file
        reader = csv.reader(f, delimiter='\t')
        next(reader)  # Skip the header row
        for row_num, row in enumerate(reader, start=1):
            try:
                data = process_row_func(row)

                # Check if process_row_func returned a list of tuples
                if isinstance(data, list):
                    for record in data:  # Insert each tuple in the list
                        cursor.execute(query, record)
                elif data:  # Single record (tuple)
                    cursor.execute(query, data)

            except psycopg2.Error as e:
                print(
                    f"Error inserting into {table_name} at row {row_num}: {e}")
                connection.rollback()
            else:
                connection.commit()
    end_time = time.time()
    elapsed_minutes = (end_time - start_time) / 60  # Convert seconds to minutes
    print(
        f"Completed loading {table_name} from {tsv_file}. Time taken: {elapsed_minutes:.2f} minutes.")


# Processing functions for each table
def process_artist_row(row):
    birthYear = row[2] if row[2] != '\\N' else None
    deathYear = row[3] if row[3] != '\\N' else None
    return (row[0], row[1], birthYear, deathYear, row[4], row[5])


def process_title_row(row):
    startYear = row[5] if row[5] != '\\N' else None
    endYear = row[6] if row[6] != '\\N' else None
    runtimeMinutes = row[7] if row[7] != '\\N' else None
    isAdult = bool(int(row[4])) if row[4] != '\\N' else False
    return (
    row[0], row[1], row[2], row[3], isAdult, startYear, endYear, runtimeMinutes)


def process_principal_row(row):
    return (row[0], int(row[1]), row[2], row[3], row[4], row[5])


def process_rating_row(row):
    return (row[0], float(row[1]), int(row[2]))


def process_title_akas_row(row):
    isOriginalTitle = bool(int(row[7])) if row[7] != '\\N' else False
    return (row[0], int(row[1]), row[2], row[3], row[4], row[5], row[6],
            isOriginalTitle)


def process_genre_row(row):
    genres = row[8].split(",") if row[8] != '\\N' else []
    # returns a list of tuples
    return [(row[0], genre.strip()) for genre in genres] if genres else None


def process_profession_row(row):
    professions = row[4].split(",") if row[4] != '\\N' else []
    return [(row[0], profession.strip()) for profession in
            professions] if professions else None


def process_known_titles_row(row):
    titles = row[5].split(",") if row[5] != '\\N' else []
    return [(row[0], title.strip()) for title in titles] if titles else None


# Insert queries
artist_insert_query = """INSERT INTO Artist (nconst, primaryName, birthYear, deathYear, primaryProfession, knownForTitles) 
                         VALUES (%s, %s, %s, %s, %s, %s)"""

title_insert_query = """INSERT INTO Title (tconst, titleType, primaryTitle, originalTitle, isAdult, startYear, endYear, runtimeMinutes) 
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"""

principal_insert_query = """INSERT INTO Principals (tconst, ordering, nconst, category, job, characters) 
                            VALUES (%s, %s, %s, %s, %s, %s)"""

rating_insert_query = """INSERT INTO Rating (tconst, averageRating, numVotes) 
                         VALUES (%s, %s, %s)"""

akas_insert_query = """INSERT INTO Title_Akas (titleID, ordering, title, region, language, types, attributes, isOriginalTitle) 
VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"""

title_genre_insert_query = """INSERT INTO Title_Genre (tconst, GenreID) 
                              VALUES (%s, (SELECT GenreID FROM Genre WHERE genreName = %s))"""

artist_profession_insert_query = """INSERT INTO Artist_Profession (nconst, Label) 
                                    VALUES (%s, %s)"""

artist_known_insert_query = """INSERT INTO Artist_Known (nconst, tconst) 
                               VALUES (%s, %s)"""

genre_insert_query = """INSERT INTO Genre (genreName) VALUES (%s) ON CONFLICT (genreName) DO NOTHING"""

# Load data into tables
insert_data_from_tsv('Artist', 'data/name.basics.tsv', artist_insert_query,
                     process_artist_row)
insert_data_from_tsv('Title', 'data/title.basics.tsv', title_insert_query,
                     process_title_row)
insert_data_from_tsv('Principals', 'data/title.principals.tsv',
                     principal_insert_query, process_principal_row)
insert_data_from_tsv('Rating', 'data/title.ratings.tsv', rating_insert_query,
                     process_rating_row)
insert_data_from_tsv('Title_Akas', 'data/title.akas.tsv', akas_insert_query,
                     process_title_akas_row)


# Insert genres first and then insert Title_Genre relationships
def insert_genres_and_title_genres():
    start_time = time.time()  # Start time
    with open('data/title.basics.tsv', 'r',
              encoding='utf-8') as f:  # Read plain TSV file
        reader = csv.reader(f, delimiter='\t')
        next(reader)  # Skip header
        for row_num, row in enumerate(reader, start=1):
            # Ensure the row has at least 9 elements to account for data
            # inconsistencies
            if len(row) < 9:
                print(f"Skipping row {row_num}: Row has fewer than 9 columns.")
                continue  # Skip rows with insufficient data

            # Get the genres column, or use an empty list if it's missing or null
            genres = row[8].split(",") if row[8] != '\\N' else []
            for genre in genres:
                try:
                    # Insert the genre into the Genre table
                    cursor.execute(genre_insert_query, (genre.strip(),))
                    # Insert into the Title_Genre table
                    cursor.execute(title_genre_insert_query,
                                   (row[0], genre.strip()))
                except psycopg2.Error as e:
                    print(
                        f"Error inserting into Genre or Title_Genre at row {row_num}: {e}")
                    connection.rollback()
                else:
                    connection.commit()
    end_time = time.time()
    elapsed_minutes = (end_time - start_time) / 60
    print(
        f"Completed loading genres and title-genre relationships. Time taken: {elapsed_minutes:.2f} minutes.")


# Call the function
insert_genres_and_title_genres()

insert_data_from_tsv('Artist_Profession', 'data/name.basics.tsv',
                     artist_profession_insert_query, process_profession_row)
insert_data_from_tsv('Artist_Known', 'data/name.basics.tsv',
                     artist_known_insert_query, process_known_titles_row)

# Close the cursor and connection
cursor.close()
connection.close()
