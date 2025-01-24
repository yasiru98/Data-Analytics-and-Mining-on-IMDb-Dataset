import pandas as pd
import os


# Cleans a TSV file by ensuring the correct number of columns, replacing
# missing values, applying transformations, and removing duplicates.
def clean_tsv(file_path, expected_columns, column_names, transformations,
              unique_identifier=None):
    try:
        # Load the TSV file
        df = pd.read_csv(file_path, sep='\t', dtype=str, header=0)

        # Ensure rows have the correct number of columns
        df = df[df.apply(lambda x: len(x) == expected_columns, axis=1)]
        df.columns = column_names  # Assign column names

        # Replace missing values
        df.replace('\\N', None, inplace=True)

        # Apply transformations to each column
        for column, transform_func in transformations.items():
            if column in df.columns:
                try:
                    df[column] = df[column].apply(transform_func)
                except ValueError as e:
                    print(f"ValueError for column {column}: {e}")

        # Remove duplicates
        if unique_identifier:
            df.drop_duplicates(subset=unique_identifier, inplace=True)

    except Exception as e:
        print(f"Error processing {file_path}: {e}")
        return None

    print(f"Cleaned {os.path.basename(file_path)}: {len(df)} rows.")
    return df


# Cleans all data files and saves cleaned versions.
def clean_all_datasets():
    data_dir = "data"  # Directory with input files
    output_dir = "cleaned_data"  # Directory for cleaned files
    os.makedirs(output_dir, exist_ok=True)

    # Clean title.akas.tsv
    clean_title_akas = clean_tsv(
        file_path=os.path.join(data_dir, "title.akas.tsv"),
        expected_columns=8,
        column_names=["titleId", "ordering", "title", "region", "language",
                      "types", "attributes", "isOriginalTitle"],
        transformations={
            "ordering": lambda x: int(x) if x is not None else None,
            "isOriginalTitle": lambda x: bool(
                int(x)) if x is not None else None,
        },
        unique_identifier=["titleId", "ordering"]
    )
    if clean_title_akas is not None:
        clean_title_akas.to_csv(
            os.path.join(output_dir, "title.akas.cleaned.tsv"), sep='\t',
            index=False)

    # Clean title.basics.tsv
    clean_title_basics = clean_tsv(
        file_path=os.path.join(data_dir, "title.basics.tsv"),
        expected_columns=9,
        column_names=["tconst", "titleType", "primaryTitle", "originalTitle",
                      "isAdult", "startYear", "endYear", "runtimeMinutes",
                      "genres"],
        transformations={
            "isAdult": lambda x: bool(int(x)) if x is not None else None,
            "startYear": lambda x: int(x) if x is not None else None,
            "endYear": lambda x: int(x) if x is not None else None,
            # check if the value is numeric to account for invalid data
            "runtimeMinutes": lambda x: int(
                x) if x is not None and x.isdigit() else None,
            # check type to account for invalid data
            "genres": lambda x: x.split(',') if isinstance(x, str) else [],
        },
        unique_identifier=["tconst"]
    )
    if clean_title_basics is not None:
        clean_title_basics.to_csv(
            os.path.join(output_dir, "title.basics.cleaned.tsv"), sep='\t',
            index=False)

    # Clean title.principals.tsv
    clean_title_principals = clean_tsv(
        file_path=os.path.join(data_dir, "title.principals.tsv"),
        expected_columns=6,
        column_names=["tconst", "ordering", "nconst", "category", "job",
                      "characters"],
        transformations={
            "ordering": lambda x: int(x) if x is not None else None,
            "characters": lambda x: " | ".join(eval(x)) if x and x.startswith(
                '[') else None,
            "job": lambda x: x if x and x != "\\N" else None,
        }
    )

    if clean_title_principals is not None:
        clean_title_principals.to_csv(
            os.path.join(output_dir, "title.principals.cleaned.tsv"), sep='\t',
            index=False
        )

    # Clean title.ratings.tsv
    clean_title_ratings = clean_tsv(
        file_path=os.path.join(data_dir, "title.ratings.tsv"),
        expected_columns=3,
        column_names=["tconst", "averageRating", "numVotes"],
        transformations={
            "averageRating": lambda x: float(x) if x is not None else None,
            "numVotes": lambda x: int(x) if x is not None else None,
        },
        unique_identifier=["tconst"]
    )
    if clean_title_ratings is not None:
        clean_title_ratings.to_csv(
            os.path.join(output_dir, "title.rating.cleaned.tsv"), sep='\t',
            index=False)

    # Clean name.basics.tsv
    clean_name_basics = clean_tsv(
        file_path=os.path.join(data_dir, "name.basics.tsv"),
        expected_columns=6,
        column_names=["nconst", "primaryName", "birthYear", "deathYear",
                      "primaryProfession", "knownForTitles"],
        transformations={
            "birthYear": lambda x: int(x) if x is not None else None,
            "deathYear": lambda x: int(x) if x is not None else None,
            "primaryProfession": lambda x: x.split(
                ',') if x is not None else [],
            "knownForTitles": lambda x: x.split(',') if x is not None else [],
        },
        unique_identifier=["nconst"]
    )
    if clean_name_basics is not None:
        clean_name_basics.to_csv(
            os.path.join(output_dir, "name.basics.cleaned.tsv"), sep='\t',
            index=False)

    print(f"All datasets cleaned and saved to {output_dir}")


clean_all_datasets()
