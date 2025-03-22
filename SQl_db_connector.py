import os
import pandas as pd
import mysql.connector
from datetime import datetime
import logging
import numpy as np
import time

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filename="csv_import.log"
)

# Configuration
FOLDER_PATH = r"C:\Users\vivek\Data science\EDA\aug1"

# MySQL connection details
MYSQL_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "",
    "database": "aug_info",
    # Add these parameters for better stability:
    "connect_timeout": 180,
    "use_pure": True
}

# List of columns that are known to be dates and their formats
DATE_COLUMNS = {
    # Example: 'order_date': '%Y-%m-%d'
    'order_estimated_delivery_date': '%Y-%m-%d %H:%M:%S',
    'review_creation_date': '%Y-%m-%d %H:%M:%S'
}

def get_all_csv_files(folder_path):
    """Get all CSV files in the folder."""
    csv_files = []
    
    for file in os.listdir(folder_path):
        if file.lower().endswith(".csv"):
            file_path = os.path.join(folder_path, file)
            csv_files.append(file_path)
    
    return csv_files

def chunk_dataframe(df, chunk_size=1000):
    """Split dataframe into chunks to avoid memory issues."""
    chunks = []
    num_chunks = len(df) // chunk_size + 1
    
    for i in range(num_chunks):
        start_idx = i * chunk_size
        end_idx = min(start_idx + chunk_size, len(df))
        chunks.append(df.iloc[start_idx:end_idx])
    
    return chunks

def import_csv_to_mysql(csv_file_path):
    """Import CSV file into MySQL database with improved error handling."""
    try:
        # Read CSV file
        print(f"🔄 Reading CSV file: {csv_file_path}...")
        
        # Get file size to check if it's large
        file_size_mb = os.path.getsize(csv_file_path) / (1024 * 1024)
        
        if file_size_mb > 100:  # If file is larger than 100MB
            print(f"⚠️ Large file detected ({file_size_mb:.2f}MB). Using chunked processing.")
            df = pd.read_csv(csv_file_path, low_memory=False)
        else:
            df = pd.read_csv(csv_file_path)
            
        # Get the file name without extension
        file_name = os.path.basename(csv_file_path)
        file_name_without_ext = os.path.splitext(file_name)[0]

        # Convert column names to safe MySQL format
        df.columns = [col.strip().replace(" ", "").replace("-", "_") for col in df.columns]

        # Handle date columns
        for col in df.columns:
            # Check if column is in our known date columns dictionary
            if col in DATE_COLUMNS:
                try:
                    df[col] = pd.to_datetime(df[col], format=DATE_COLUMNS[col], errors="coerce").dt.date
                    print(f"✓ Converted column '{col}' to date using format {DATE_COLUMNS[col]}")
                except Exception as e:
                    logging.warning(f"Could not convert column {col} to date: {e}")
            # Try to detect date columns, but only for reasonable column sizes (avoiding ID columns)
            elif df[col].dtype == "object" and df[col].nunique() < len(df) * 0.8:
                # Skip likely text columns or columns with many NaN values
                non_null_values = df[col].dropna()
                if len(non_null_values) > 0 and isinstance(non_null_values.iloc[0], str):
                    # Only try to convert if the column looks like it might contain dates
                    sample_val = non_null_values.iloc[0]
                    if len(sample_val) >= 6 and (
                        "/" in sample_val or "-" in sample_val or "." in sample_val
                    ):
                        try:
                            df[col] = pd.to_datetime(df[col], errors="coerce", dayfirst=True).dt.date
                            # Count success rate
                            success_rate = df[col].notna().mean()
                            if success_rate < 0.5:  # If less than 50% converted, revert
                                print(f"⚠️ Column '{col}' had low date conversion rate ({success_rate:.2%}), keeping as text")
                                df[col] = df[col].astype("object")
                            else:
                                print(f"✓ Auto-detected and converted column '{col}' to date format")
                        except Exception as e:
                            pass  # Silently continue if not a date

        # Replace NaN with None
        df = df.replace({np.nan: None})

        # Generate table name
        current_date = datetime.now()
        table_name = f"data_{current_date.year}{current_date.month:02d}{file_name_without_ext}"
        table_name = "".join(c if c.isalnum() or c == "" else "" for c in table_name)

        # Connect to MySQL
        print(f"🔄 Connecting to MySQL database...")
        conn = mysql.connector.connect(**MYSQL_CONFIG)
        cursor = conn.cursor()

        # Create table dynamically
        columns = []
        for col in df.columns:
            if pd.api.types.is_integer_dtype(df[col]):
                col_type = "BIGINT"
            elif pd.api.types.is_float_dtype(df[col]):
                col_type = "DECIMAL(18,4)"
            # Fix the isinstance check by directly checking if it's a date type
            elif (
                not df[col].empty 
                and df[col].iloc[0] is not None 
                and hasattr(df[col].iloc[0], 'strftime')  # Check if it's date-like
            ):
                col_type = "DATE"
            else:
                # Get max length to optimize VARCHAR size
                if df[col].dtype == "object":
                    # Get max length, but handle potential NoneType values
                    non_null = df[col].dropna()
                    max_len = 0
                    if len(non_null) > 0:
                        max_len = non_null.astype(str).str.len().max()
                    
                    if max_len and max_len < 255:
                        col_type = f"VARCHAR({max_len + 50})"  # Add buffer
                    else:
                        col_type = "TEXT"
                else:
                    col_type = "TEXT"

            columns.append(f"{col} {col_type}")

        print(f"🔄 Creating table {table_name}...")
        create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} ({', '.join(columns)})"
        cursor.execute(create_table_query)

        # Estimate the number of rows to process
        total_rows = len(df)
        print(f"🔄 Inserting {total_rows} rows into {table_name}...")
        
        # Process in chunks to avoid memory issues with large datasets
        if total_rows > 10000:
            chunks = chunk_dataframe(df, chunk_size=5000)
            print(f"🔄 Processing in {len(chunks)} chunks...")
            
            for i, chunk in enumerate(chunks):
                rows_inserted = 0
                
                # Begin transaction for this chunk
                conn.start_transaction()
                
                try:
                    for _, row in chunk.iterrows():
                        placeholders = ", ".join(["%s"] * len(row))
                        columns = ", ".join([f"{col}" for col in df.columns])
                        insert_query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
                        cursor.execute(insert_query, tuple(row))
                        rows_inserted += 1
                        
                        # Print progress update for larger chunks
                        if rows_inserted % 1000 == 0:
                            print(f"  ↳ Inserted {rows_inserted}/{len(chunk)} rows in chunk {i+1}/{len(chunks)}")
                    
                    # Commit this chunk
                    conn.commit()
                    print(f"✅ Chunk {i+1}/{len(chunks)} committed: {rows_inserted} rows")
                    
                except Exception as e:
                    # Rollback on error
                    conn.rollback()
                    logging.error(f"Error in chunk {i+1}: {str(e)}")
                    print(f"❌ Error in chunk {i+1}: {str(e)}")
                    # Continue to the next chunk
        else:
            # For smaller datasets, insert all at once
            for idx, row in enumerate(df.itertuples(index=False), 1):
                placeholders = ", ".join(["%s"] * len(df.columns))
                columns = ", ".join([f"{col}" for col in df.columns])
                insert_query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
                cursor.execute(insert_query, tuple(row))
                
                # Print progress periodically
                if idx % 1000 == 0 or idx == total_rows:
                    print(f"  ↳ Processed {idx}/{total_rows} rows ({idx/total_rows:.1%})")
                    # Commit periodically to avoid huge transactions
                    conn.commit()

        # Final commit
        conn.commit()
        cursor.close()
        conn.close()

        logging.info(f"Successfully imported {csv_file_path} to MySQL table {table_name}")
        print(f"✅ Successfully imported {csv_file_path} to MySQL table {table_name}")
        return True
    except KeyboardInterrupt:
        print("\n⚠️ Process was interrupted by user. Cleaning up...")
        try:
            if 'conn' in locals() and conn.is_connected():
                conn.rollback()
                cursor.close()
                conn.close()
        except:
            pass
        logging.warning(f"Import process for {csv_file_path} was interrupted by user")
        return False
    except Exception as e:
        logging.error(f"❌ Failed to import {csv_file_path}: {str(e)}")
        print(f"❌ Error importing {csv_file_path}: {str(e)}")
        # Try to clean up database connection
        try:
            if 'conn' in locals() and conn.is_connected():
                conn.rollback()
                cursor.close()
                conn.close()
        except:
            pass
        return False

def main():
    """Main function to import all CSV files."""
    start_time = time.time()
    print(f"📂 Looking for CSV files in folder: {FOLDER_PATH}")
    logging.info(f"Script started - looking for CSV files in: {FOLDER_PATH}")
    
    # Get all CSV files in the folder
    csv_files = get_all_csv_files(FOLDER_PATH)
    
    if csv_files:
        print(f"📂 Found {len(csv_files)} CSV files to process.")
        
        # Process each file
        success_count = 0
        for i, file_path in enumerate(csv_files, 1):
            print(f"\n📄 Processing CSV file [{i}/{len(csv_files)}]: {file_path}")
            logging.info(f"Processing CSV file: {file_path}")
            
            if import_csv_to_mysql(file_path):
                success_count += 1
                
        # Print summary
        elapsed_time = time.time() - start_time
        print(f"\n✅ Import complete! Successfully imported {success_count}/{len(csv_files)} files.")
        print(f"⏱️ Total execution time: {elapsed_time/60:.2f} minutes")
        logging.info(f"Script finished - imported {success_count}/{len(csv_files)} files in {elapsed_time/60:.2f} minutes")
    else:
        print("📂 No CSV files found in the folder.")
        logging.info("No CSV files found in the folder.")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n⚠️ Process was interrupted by user.")
        logging.warning("Script execution interrupted by user")
    except Exception as e:
        print(f"\n❌ Unexpected error: {str(e)}")
        logging.error(f"Unexpected error: {str(e)}")