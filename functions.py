import polars as pl
import logging


logger = logging.getLogger(__name__)

def load_data(file_path):
    df = pl.read_csv(file_path, infer_schema_length=1000)
    logger.info(f"Data loaded from {file_path} with shape {df.shape}")
    return df

def clean_and_transform_data(df):
    # Convert 'Gross' column to numeric
    df = df.with_columns(
        pl.col('Gross').str.replace_all(',', '').cast(pl.Float64)
    )
    logger.info(f"Data cleaned and transformed. New shape: {df.shape}")
    return df

def save_data(df, output_path):
    df.write_csv(output_path)
    logger.info(f"Data saved to {output_path}")
    