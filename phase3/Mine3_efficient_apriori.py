import psycopg2
import pandas as pd
from efficient_apriori import apriori
from collections import defaultdict
import sys
import os

# Database connection parameters
db_params = {
    'dbname': 'project2',
    'user': 'postgres',
    'password': '114514',
    'host': 'localhost',
    'port': 5432
}

def fetch_rating_data(cursor):
    """
    Fetches rating data from the PostgreSQL database.

    Returns:
        List of transactions, where each transaction is a list of rating categories associated with an artist.
    """
    try:
        # Fetch rating categories for each artist from artist_known, title, and rating tables
        query = """
            SELECT ak.nconst, 
                   CASE 
                       WHEN r.averagerating >= 6.5 THEN 'high_rating'
                       ELSE 'low_rating'
                   END AS rating_category
            FROM artist_known ak
            JOIN title t ON ak.tconst = t.tconst
            JOIN rating r ON t.tconst = r.tconst
            WHERE r.averagerating IS NOT NULL;
        """
        cursor.execute(query)
        records = cursor.fetchall()

        # Organize data into a dictionary with nconst as keys and list of rating categories as values
        transactions = defaultdict(list)
        for nconst, rating_category in records:
            transactions[nconst].append(rating_category)

        # Convert the dictionary to a list of rating category lists
        transaction_list = list(transactions.values())
        return transaction_list

    except psycopg2.Error as e:
        print("An error occurred while fetching rating data:")
        print(e)
        return []

def save_itemsets_to_csv(itemsets, k, total_transactions, dataset='ratings'):
    """
    Saves frequent itemsets of size k to a CSV file.

    Args:
        itemsets (dict): Dictionary of frequent itemsets with their counts.
        k (int): The size of the itemsets.
        total_transactions (int): Total number of transactions for absolute count calculation.
        dataset (str): The name of the dataset (professions, genres, ratings).
    """
    if k not in itemsets or not itemsets[k]:
        print(f"No frequent {k}-itemsets to save for {dataset}.")
        return

    filename = f"l{k}_{dataset}_efficient_apriori.csv"
    subset = []

    for itemset, count in itemsets[k].items():
        if k == 1:
            subset.append({'rating_category': list(itemset)[0], 'count': count})
        elif k == 2:
            subset.append({
                'rating_category1': list(itemset)[0],
                'rating_category2': list(itemset)[1],
                'count': count
            })
        elif k == 3:
            subset.append({
                'rating_category1': list(itemset)[0],
                'rating_category2': list(itemset)[1],
                'rating_category3': list(itemset)[2],
                'count': count
            })
        else:
            # For k > 3, dynamically create keys
            item_keys = {f'rating_category{i+1}': prof for i, prof in enumerate(itemset)}
            item_keys['count'] = count
            subset.append(item_keys)

    df = pd.DataFrame(subset)
    df.to_csv(filename, index=False)
    print(f"Frequent {k}-itemsets saved to '{filename}' ({len(subset)} itemsets).")

def main():
    try:
        # Connect to the PostgreSQL database
        conn = psycopg2.connect(**db_params)
        cursor = conn.cursor()
        print("Connected to the database successfully.\n")

        # Fetch rating transactions
        print("Fetching rating data...")
        transactions = fetch_rating_data(cursor)
        total_transactions = len(transactions)
        print(f"Number of transactions (artists): {total_transactions}")
        if transactions:
            print(f"Sample transaction: {transactions[0]}\n")
        else:
            print("No transactions found.\n")

        if not transactions:
            print("No data available to perform Apriori analysis. Exiting.")
            return

        # Define minimum support
        min_support_absolute = 100  # Changed threshold to 100
        min_support = min_support_absolute / total_transactions
        print(f"Applying Apriori algorithm with min_support = {min_support:.8f} (absolute support >= {min_support_absolute})\n")

        # Define maximum k
        max_k = 50  # You can adjust this as needed

        # Initialize current level
        current_k = 1

        # Create output directory if it doesn't exist
        output_dir = "apriori_ratings_results"
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        os.chdir(output_dir)

        while current_k <= max_k:
            print(f"Processing L{current_k}_ratings...")
            # Apply Apriori algorithm using efficient_apriori
            # For higher k, set max_length to current_k
            itemsets, _ = apriori(transactions, min_support=min_support, min_confidence=0.0, max_length=current_k)

            # Check if there are any frequent itemsets of size k
            if current_k not in itemsets or not itemsets[current_k]:
                print(f"No frequent {current_k}-itemsets found in L{current_k}_ratings.")
                break  # Terminate if no frequent itemsets found at this level

            # Save frequent itemsets to CSV
            save_itemsets_to_csv(itemsets, current_k, total_transactions, dataset='ratings')

            # Increment level
            current_k += 1

        print("\nApriori analysis for ratings completed.")

        # Close the cursor and connection
        cursor.close()
        conn.close()
        print("\nDatabase connection closed.")

    except MemoryError:
        print("MemoryError: The script ran out of memory. Consider optimizing the data processing steps or using a machine with more RAM.")
    except psycopg2.Error as e:
        print("An error occurred while connecting to the database:")
        print(e)
    except Exception as ex:
        print("An unexpected error occurred:")
        print(ex)

if __name__ == "__main__":
    main()
