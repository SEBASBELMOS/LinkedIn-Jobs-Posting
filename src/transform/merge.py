import pandas as pd
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(message)s",
    datefmt="%d/%m/%Y %I:%M:%S %p"
)

def merge_data(dataframes, reviews_df):
    """
    Merge the transformed database data with the transformed Glassdoor API data.
    
    Args:
        dataframes (dict): Dictionary of transformed DataFrames from the database.
        reviews_df (pd.DataFrame): Transformed DataFrame from the Glassdoor API.
    
    Returns:
        dict: Updated dictionary of DataFrames with the merged data.
    """
    logging.info("Starting merge_data: Merging database and Glassdoor API data.")
    
    if 'jobs' in dataframes:
        jobs_df = dataframes['jobs']
        merged_df = jobs_df.merge(
            reviews_df,
            on='company_name',
            how='left'
        )
        for col in ['overall_rating', 'culture_and_values', 'work_life_balance',
                    'senior_leadership', 'compensation_and_benefits', 'career_opportunities']:
            merged_df[col] = merged_df[col].fillna(0)
        dataframes['jobs'] = merged_df
    
    logging.info("merge_data completed successfully.")
    return dataframes