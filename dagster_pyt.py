import pandas as pd
from dagster import op, job, resource, Field, String
from sqlalchemy import create_engine

# Step 1: Define Resources (for database connection)

@resource(config_schema={"db_url": Field(String)})
def sqlite_resource(context):
    engine = create_engine(context.resource_config["db_url"])
    return engine

# Step 2: Define Operations (Extract, Transform, Load)

# Extract operation: reads data from a CSV file
@op(config_schema={"file_path": Field(String)})
def extract_data(context):
    file_path = context.op_config["file_path"]
    df = pd.read_csv(file_path)
    context.log.info(f"Extracted {len(df)} rows from {file_path}")
    return df

# Transform operation: cleans data (e.g., removes rows with missing values)
@op
def transform_data(context, df: pd.DataFrame):
    context.log.info("Cleaning data...")
    clean_df = df.dropna()  # Drop rows with null values as an example transformation
    context.log.info(f"Cleaned data to {len(clean_df)} rows")
    return clean_df

# Load operation: writes the cleaned data into the database (SQLite)
@op(required_resource_keys={"db"})
def load_data(context, df: pd.DataFrame):
    engine = context.resources.db
    df.to_sql('cleaned_data', con=engine, if_exists='replace', index=False)
    context.log.info("Data successfully loaded into the database.")

# Step 3: Define the Job (Pipeline)

@job(resource_defs={"db": sqlite_resource})
def etl_pipeline():
    df = extract_data()
    clean_df = transform_data(df)
    load_data(clean_df)

# Step 4: Define the Main Entry Point

if __name__ == "__main__":
    from dagster import execute_job
    execute_job(etl_pipeline)