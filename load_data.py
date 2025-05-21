import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
import os
from datetime import datetime
import re
import h3

# Database connection parameters
DB_PARAMS = {
    'dbname': 'airbnb',
    'user': 'postgres',
    'password': 'postgres',
    'host': 'localhost',
    'port': '5435'
}

def clean_price(price_str):
    """Clean price string and convert to integer."""
    if pd.isna(price_str):
        return None
    # Remove currency symbol and commas, then convert to integer
    return float(re.sub(r'[$,]', '', str(price_str)))

def clean_date(date_str):
    """Clean date string and convert to datetime."""
    if pd.isna(date_str):
        return None
    return pd.to_datetime(date_str).date()

def load_listings():
    """Load listings data into the database."""
    print("Loading listings data...")
    df = pd.read_csv('code and data/data/airbnb/29-march-2023/listings.csv')
    
    # Clean and prepare data
    df['price'] = df['price'].apply(clean_price)
    df['last_review'] = df['last_review'].apply(clean_date)
    
    # Calculate H3 cell identifier
    df['h3_cell_res9'] = df.apply(lambda row: h3.latlng_to_cell(row['latitude'], row['longitude'], 9), axis=1)
    
    # Connect to database
    with psycopg2.connect(**DB_PARAMS) as conn:
        with conn.cursor() as cur:
            # Prepare data for insertion
            data = df.to_dict('records')
            
            # Create the insert query
            insert_query = """
                INSERT INTO airbnb.listings (
                    id, name, host_id, host_name, neighbourhood,
                    latitude, longitude, h3_cell_res9, room_type, price, minimum_nights,
                    number_of_reviews, last_review, reviews_per_month,
                    calculated_host_listings_count, availability_365,
                    number_of_reviews_ltm, license
                ) VALUES %s
                ON CONFLICT (id) DO UPDATE SET
                    name = EXCLUDED.name,
                    host_id = EXCLUDED.host_id,
                    host_name = EXCLUDED.host_name,
                    neighbourhood = EXCLUDED.neighbourhood,
                    latitude = EXCLUDED.latitude,
                    longitude = EXCLUDED.longitude,
                    h3_cell_res9 = EXCLUDED.h3_cell_res9,
                    room_type = EXCLUDED.room_type,
                    price = EXCLUDED.price,
                    minimum_nights = EXCLUDED.minimum_nights,
                    number_of_reviews = EXCLUDED.number_of_reviews,
                    last_review = EXCLUDED.last_review,
                    reviews_per_month = EXCLUDED.reviews_per_month,
                    calculated_host_listings_count = EXCLUDED.calculated_host_listings_count,
                    availability_365 = EXCLUDED.availability_365,
                    number_of_reviews_ltm = EXCLUDED.number_of_reviews_ltm,
                    license = EXCLUDED.license
            """
            
            # Prepare the data for execute_values
            values = [(
                row['id'],
                row['name'],
                row['host_id'],
                row['host_name'],
                row['neighbourhood'],
                row['latitude'],
                row['longitude'],
                row['h3_cell_res9'],
                row['room_type'],
                row['price'],
                row['minimum_nights'],
                row['number_of_reviews'],
                row['last_review'],
                row['reviews_per_month'],
                row['calculated_host_listings_count'],
                row['availability_365'],
                row['number_of_reviews_ltm'],
                row['license']
            ) for row in data]
            
            # Execute the insert
            execute_values(cur, insert_query, values)
            conn.commit()
            print(f"Loaded {len(values)} listings records")

def load_reviews():
    """Load reviews data into the database."""
    print("Loading reviews data...")
    df = pd.read_csv('code and data/data/airbnb/29-march-2023/reviews.csv')
    
    # Clean and prepare data
    df['date'] = df['date'].apply(clean_date)
    
    # Connect to database
    with psycopg2.connect(**DB_PARAMS) as conn:
        with conn.cursor() as cur:
            # Prepare data for insertion
            data = df.to_dict('records')
            
            # Create the insert query
            insert_query = """
                INSERT INTO airbnb.reviews (listing_id, date)
                VALUES %s
                ON CONFLICT DO NOTHING
            """
            
            # Prepare the data for execute_values
            values = [(row['listing_id'], row['date']) for row in data]
            
            # Execute the insert
            execute_values(cur, insert_query, values)
            conn.commit()
            print(f"Loaded {len(values)} reviews records")

def load_calendar():
    """Load calendar data into the database with price filtering using chunked processing."""
    print("Loading calendar data...")
    
    # Define chunk size
    chunk_size = 100000  # Process 100k records at a time
    
    # Initialize counters
    total_processed = 0
    total_filtered = 0
    price_threshold = 10000000
    
    # Connect to database
    with psycopg2.connect(**DB_PARAMS) as conn:
        with conn.cursor() as cur:
            # Process the CSV file in chunks
            for chunk in pd.read_csv('code and data/data/airbnb/29-march-2023/calendar.csv.gz', chunksize=chunk_size):
                # Clean and prepare data
                chunk['date'] = chunk['date'].apply(clean_date)
                chunk['available'] = chunk['available'].map({'t': True, 'f': False})
                chunk['price'] = chunk['price'].apply(clean_price)
                chunk['adjusted_price'] = chunk['adjusted_price'].apply(clean_price)
                
                # Filter out rows with prices over threshold
                filtered_chunk = chunk[
                    (chunk['price'] <= price_threshold) & 
                    (chunk['adjusted_price'] <= price_threshold)
                ]
                
                # Update counters
                total_processed += len(chunk)
                total_filtered += len(chunk) - len(filtered_chunk)
                
                # Prepare data for insertion
                data = filtered_chunk.to_dict('records')
                
                # Create the insert query
                insert_query = """
                    INSERT INTO airbnb.calendar (
                        listing_id, date, available, price,
                        adjusted_price, minimum_nights, maximum_nights
                    ) VALUES %s
                    ON CONFLICT (listing_id, date) DO UPDATE SET
                        available = EXCLUDED.available,
                        price = EXCLUDED.price,
                        adjusted_price = EXCLUDED.adjusted_price,
                        minimum_nights = EXCLUDED.minimum_nights,
                        maximum_nights = EXCLUDED.maximum_nights
                """
                
                # Prepare the data for execute_values
                values = [(
                    row['listing_id'],
                    row['date'],
                    row['available'],
                    row['price'],
                    row['adjusted_price'],
                    row['minimum_nights'],
                    row['maximum_nights']
                ) for row in data]
                
                # Execute the insert
                execute_values(cur, insert_query, values)
                conn.commit()
                
                # Report progress
                print(f"Processed {total_processed:,} records...")
                print(f"Filtered out {total_filtered:,} records with prices over {price_threshold:,}")
                print(f"Successfully loaded {len(values):,} records in this chunk")
                print("-" * 50)
    
    print(f"\nFinal Summary:")
    print(f"Total records processed: {total_processed:,}")
    print(f"Total records filtered out: {total_filtered:,}")
    print(f"Total records loaded: {total_processed - total_filtered:,}")


def main():
    try:
        # Load data in the correct order
        load_listings()
        load_reviews()
        load_calendar()
        print("Data loading completed successfully!")
    except Exception as e:
        print(f"Error loading data: {str(e)}")

if __name__ == "__main__":
    main() 