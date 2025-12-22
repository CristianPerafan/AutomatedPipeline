import logging

from functions import (
    clean_and_transform_data,
    load_data,
    save_data
)

logging.basicConfig(level=logging.INFO)

# 1. Load data
df = load_data('data/imdb_top_1000.csv')


# 2. Clean and transform data
df_cleaned = clean_and_transform_data(df)

# 3. Save cleaned data
save_data(df_cleaned, 'data/imdb_top_1000_cleaned.csv')
