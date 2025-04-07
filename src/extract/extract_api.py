# import pandas as pd
# import requests
# import logging
# import os
# from dotenv import load_dotenv

# load_dotenv()

# logging.basicConfig(
#     level=logging.INFO,
#     format="%(asctime)s %(message)s",
#     datefmt="%d/%m/%Y %I:%M:%S %p"
# )

# GLASSDOOR_API_URL = "https://real-time-glassdoor-data.p.rapidapi.com/company/reviews"
# GLASSDOOR_API_KEY = os.getenv("GLASSDOOR_API_KEY")
# GLASSDOOR_API_HOST = os.getenv("GLASSDOOR_API_HOST")

# if not GLASSDOOR_API_KEY or not GLASSDOOR_API_HOST:
#     raise ValueError("Glassdoor API credentials (GLASSDOOR_API_KEY, GLASSDOOR_API_HOST) must be set in the .env file.")

# def extract_api_data(jobs_df, run_id):
#     """
#     Extract company reviews from the Glassdoor Data API and save as a CSV backup.
#     
#     Args:
#         jobs_df (pd.DataFrame): DataFrame containing job postings with company names.
#         run_id (str): Airflow run ID for naming the backup file.
#     
#     Returns:
#         pd.DataFrame: DataFrame containing the extracted Glassdoor reviews.
#     """
#     try:
#         logging.info("Extracting data from Glassdoor Data API.")
#         
#         unique_companies = jobs_df['company_name'].dropna().unique()
#         
#         headers = {
#             "X-RapidAPI-Key": GLASSDOOR_API_KEY,
#             "X-RapidAPI-Host": GLASSDOOR_API_HOST
#         }
#         company_reviews = []
#         for company in unique_companies:
#             try:
#                 params = {"company_name": company}
#                 response = requests.get(GLASSDOOR_API_URL, headers=headers, params=params)
#                 response.raise_for_status()
#                 if response.status_code == 200:
#                     data = response.json()
#                     if data and "reviews" in data:
#                         for review in data["reviews"]:
#                             company_reviews.append({
#                                 'company_name': company,
#                                 'overall_rating': review.get('overall_rating', 0),
#                                 'culture_and_values': review.get('culture_and_values', 0),
#                                 'work_life_balance': review.get('work_life_balance', 0),
#                                 'senior_leadership': review.get('senior_leadership', 0),
#                                 'compensation_and_benefits': review.get('compensation_and_benefits', 0),
#                                 'career_opportunities': review.get('career_opportunities', 0),
#                                 'review_date': review.get('review_date', None)
#                             })
#                     else:
#                         logging.warning(f"No reviews found for company: {company}")
#                 else:
#                     logging.warning(f"Failed to fetch data for company: {company}, Status Code: {response.status_code}")
#             except requests.exceptions.HTTPError as http_err:
#                 logging.error(f"HTTP error fetching data for company {company}: {str(http_err)}")
#                 raise
#             except Exception as e:
#                 logging.error(f"Error fetching data for company {company}: {str(e)}")
#                 continue
#         
#         reviews_df = pd.DataFrame(company_reviews)
#         
#         project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
#         backup_dir = os.path.join(project_root, "backups")
#         os.makedirs(backup_dir, exist_ok=True)
#         backup_file = os.path.join(backup_dir, f"glassdoor_reviews_{run_id}.csv")
#         reviews_df.to_csv(backup_file, index=False)
#         logging.info(f"Saved Glassdoor API data to backup CSV: {backup_file}")
#         
#         return reviews_df
#     except Exception as e:
#         logging.error(f"Error in extract_api_data: {str(e)}")
#         raise