import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime


class ETLProcess:
    def __init__(self, db_url):
        self.engine = create_engine(db_url)

    def extract(self, file_path):
        """Extracts data from the CSV file."""
        try:
            df = pd.read_csv(file_path, sep='|')
            print(df.head())
            return df
        except Exception as e:
            raise Exception(f"Error extracting data: {e}")

    def transform(self, df):
        """Transforms the data by validating and deriving fields."""
        # Drop records with missing mandatory fields
        df.dropna(subset=['customer_id', 'dob'], inplace=True)
        print("After dropping missing mandatory fields:", df)

        # Check data types before conversion
        print("Data types before conversion:", df.dtypes)

        # Convert date formats and validate
        for date_column in ['open_date', 'last_consulted_date', 'dob']:
            # Print raw data for the date column
            print(f"Raw data for {date_column} before conversion:", df[date_column])
            
            # Use errors='coerce' to convert invalid dates to NaT
            df[date_column] = pd.to_datetime(df[date_column].astype(str).str.strip(), errors='coerce')

            # Check conversion results
            print(f"Converted data for {date_column}:", df[date_column])

        # Filter out invalid dates
        df = df[df[['open_date', 'last_consulted_date', 'dob']].notna().all(axis=1)]
        print("After filtering out invalid dates:", df)

        # Calculate Age (current year - birth year)
        current_year = datetime.now().year
        df['age'] = df['dob'].apply(lambda x: current_year - x.year if pd.notnull(x) else None)

        # Convert age to integers and handle NaN
        df['age'] = df['age'].fillna(-1).astype(int)

        # Filter out records with invalid or negative ages
        df = df[df['age'] >= 0]
        print("After filtering negative ages:", df)

        # Calculate Days Since Last Consultation
        df['days_since_last_consultation'] = (datetime.now() - df['last_consulted_date']).dt.days

        # Filter to include only those with days since last consultation greater than 30
        df = df[df['days_since_last_consultation'] > 30]
        print("After filtering for days since last consultation > 30:", df)

        # Validate unique customer IDs
        if df['customer_id'].is_unique is False:
            raise ValueError("Duplicate customer IDs found.")

        print("Final transformed DataFrame:", df)
        return df


    def load(self, df):
        """Loads the transformed data into the database."""
        try:
            print("inside load")
            df.to_sql('customers_staging', self.engine, if_exists='append', index=False, chunksize=10000)
            print("loaded")
        except Exception as e:
            raise Exception(f"Error loading data: {e}")


if __name__ == "__main__":
    etl = ETLProcess(db_url='postgresql://postgres:lord@localhost:5432/etldb')
    df = etl.extract('data/customer_data.csv')
    transformed_df = etl.transform(df)
    etl.load(transformed_df)
