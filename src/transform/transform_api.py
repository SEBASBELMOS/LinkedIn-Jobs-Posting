# import pandas as pd
# import logging

# logging.basicConfig(
#     level=logging.INFO,
#     format="%(asctime)s %(message)s",
#     datefmt="%d/%m/%Y %I:%M:%S %p"
# )

# def transform_api_data(reviews_df):
#     """
#     Transform the data extracted from the Glassdoor API.
#     """
#     logging.info("Starting transform_api_data: Transforming Glassdoor API data.")
#     
#     if reviews_df.empty:
#         logging.warning("No Glassdoor API data to transform. Returning empty DataFrame.")
#         return pd.DataFrame(columns=[
#             'company_name', 'overall_rating', 'culture_and_values', 'work_life_balance',
#             'senior_leadership', 'compensation_and_benefits', 'career_opportunities'
#         ])
#     
#     reviews_df['overall_rating'] = reviews_df['overall_rating'].astype(float)
#     reviews_df['culture_and_values'] = reviews_df['culture_and_values'].astype(float)
#     reviews_df['work_life_balance'] = reviews_df['work_life_balance'].astype(float)
#     reviews_df['senior_leadership'] = reviews_df['senior_leadership'].astype(float)
#     reviews_df['compensation_and_benefits'] = reviews_df['compensation_and_benefits'].astype(float)
#     reviews_df['career_opportunities'] = reviews_df['career_opportunities'].astype(float)
#     
#     if 'review_date' in reviews_df.columns:
#         reviews_df['review_date'] = pd.to_datetime(reviews_df['review_date'], errors='coerce')
#     
#     reviews_agg = reviews_df.groupby('company_name').agg({
#         'overall_rating': 'mean',
#         'culture_and_values': 'mean',
#         'work_life_balance': 'mean',
#         'senior_leadership': 'mean',
#         'compensation_and_benefits': 'mean',
#         'career_opportunities': 'mean'
#     }).reset_index()
#     
#     logging.info(f"transform_api_data completed: {len(reviews_agg)} companies processed.")
#     return reviews_agg