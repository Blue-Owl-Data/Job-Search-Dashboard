######################## Introduction ###########################
'''
This py file prepares web developer job posts
for data exploration.
'''
###################### Import Libraries #########################
'''General Libraries'''
import numpy as np
import pandas as pd
import sys
'''AWS S3 Libraries'''
import logging
import boto3
from botocore.exceptions import ClientError

'''Regex Library'''
import re

'''Time-related Libraries'''
import time
from datetime import date
import datetime
sys.path.insert(1, './notebook/')
import MVP_Bojado

###################### Build Helper Functions ####################
def compute_post_date(df):
    '''
    This function computes the date of the job post based on post age
    and set the date as the index of the dataframe.
    '''
    # Create an empty list to hold the post date
    post_date = []
    # Create a variable to store today's date
    today = datetime.date.today()
    # For loop the column post_age and convert the values to date
    for age in df.post_age:
        if age.isin(['Just posted', 'Today']):
            post_date.append(today)
        else:
            # Extract the number
            num = re.findall(r'(\d+)', age)[0]
            # Cast the string number to integer
            num = int(num)
            # Convert the integer to timedelta object
            num = datetime.timedelta(days=num)
            # Compute post date
            date = today - num
            post_date.append(date)
    # Add post date as new column
    df['date'] = post_date
    # Set the column post_date as the index and sort the values
    df = df.set_index('date').sort_index(ascending=False)
    return df


def remove_duplicates(df):
    '''
    This function removes the duplicates in the dataframe
    '''
    # Define the columns for identifying duplicates
    columns = ['title', 'location', 'company', 'job_link', 'job_description']
    # Drop the duplicates except for the last occurrence
    df.drop_duplicates(subset=columns, inplace=True, keep='last')
    return df


def daily_update_wd():
    '''
    This function updates job posts of data scientist in TX by adding the daily acquring
    of data scientist job posts in TX. 
    '''
    # Read the job posts of data scientist in TX
    download_from_S3_bucket()
    df_ds_tx = pd.read_csv("df_wd_tx.csv")
    num_jobs = df_ds_tx.shape[0]
    # Convert the date column to datetime type
    df_ds_tx.date = pd.to_datetime(df_ds_tx.date)
    # Set the date column as the index and sort the index
    df_ds_tx = df_ds_tx.set_index('date').sort_index(ascending=False)
    
    # Get current date
    today = date.today()
    # Conver the datetime to string format
    today = today.strftime('%m%d%Y')
    # Name of file to be uploaded to S3 bucket, `dsrawjobpostings`
    file_name = "wd_tx_indeed_" + today + ".csv"
    
    df = pd.read_csv(file_name, index_col=0) # Add `index_col=0` to prevent from `Unnamed:0` bug
    
    # Add the daily update
    df = compute_post_date(df)
    df_ds_tx = pd.concat([df_ds_tx, df]).sort_index(ascending=False)
    # Remove the duplicates
    df_ds_tx = remove_duplicates(df_ds_tx)
    # Save as csv file
    df_ds_tx.to_csv("df_wd_tx.csv")
    # Upload the updated data science job postings to S3
    upload_to_S3_bucket("df_wd_tx.csv", bucket='wdrawjobpostings', object_name=None)
    # Print the new jobs posted today
    num_new_jobs = df_ds_tx.shape[0] - num_jobs
    print("New Jobs Posted Today: ", num_new_jobs)
    return df_ds_tx


def prepare_job_posts_indeed():
    '''
    The function reads the csv file of job posts and returns a cleaned dataframe
    ready for exploration.
    '''
    # Read the job posts of web developer in TX
    df = pd.read_csv("df_wd_tx.csv")

    '''
    The codes below are inactivated because using 'records' for `orient` does not preserved the index labels.
    '''
    # # Convert the string date to datetime object
    # df.date = pd.to_datetime(df.date)
    # # Set the date as the index and sort the dataframe in descending order
    # df = df.set_index('date').sort_index(ascending=False)
    # Create columns of city, state, and zipcode
    '''
    The codes above are inactivated because using 'records' for `orient` does not preserved the index labels.
    '''
    
    location = df.location.str.split(', ', expand=True)
    location.columns = ['city', 'zipcode']
    location.city = location.city.apply(lambda i: 0 if i == 'United States' else i)
    location.city = location.city.apply(lambda i: 0 if i == 'Texas' else i)
    location.zipcode = location.zipcode.apply(lambda i: 0 if re.findall(r"(\d+)", str(i)) == [] 
                                          else re.findall(r"(\d+)", str(i))[0])
    df['city'] = location.city
    df['state'] = 'TX'
    df['zipcode'] = location.zipcode
    # Replace the missing values in the company rating with 0
    df.company_rating = df.company_rating.apply(lambda i: 0 if i == 'missing' else i)
    # Drop the column post_age
    df = df.drop(columns=['post_age', 'location'])
    # Clean the text in the job description
    df = MVP_Bojado.prep_job_description_data(df, 'job_description')
    # Save a JSON version of the prepared data
    df.to_json('df_wd_tx_prepared.json', orient='records')
    return df

###################### Download and Upload Job Postings to AWS S3 ####################
def list_bucket_files(bucket_name='wdrawjobpostings'):
    '''
    This function lists all files inside of a bucket
    
    
    Parameters
    ----------
    bucket_name : str, default='wdrawjobpostings'
        The name of the AWS S3 bucket that contains the
        raw web developer job postings.
        
    Returns
    -------
    files : list
        A list of file names contained within the specified
        AWS S3 bucket.
    '''
    # Select the raw web developer job posting bucket.
    wd_job_bucket = s3.Bucket(bucket_name)
    
    # Iterate through each file in the bucket and display the name
    files = []
    for page in wd_job_bucket.objects.pages():
        for obj in page:
            print(obj.key)
            # Append the file name to the list of file names.
            files.append(obj.key)
    # Return the list of file names available in the bucket..
    return files


def download_from_S3_bucket():
    '''
    This function downloads raw web developer job postings from an AWS S3 bucket.
    The data is stored as a CSV file in the local directory.
    
    Parameters
    ---------
    None
    
    Returns
    -------
    None
    '''
    s3 = boto3.client('s3')
    file_to_download = list_bucket_files()[0]
    s3.download_file('wdrawjobpostings', file_to_download, 'df_wd_tx.csv')
    

def upload_to_S3_bucket(file_name, bucket='wdpreparedjobpostings', object_name=None):
    """
    Upload a file to an S3 bucket
    
    ***Prepared data files must be in JSON format***

    Parameters
    ----------
    file_name: str
        Name of the file to upload.
    
    bucket: str, default="wdpreparedjobpostings"
        S3 Bucket the file will be uploaded to.
    
    object_name: str, default=None
        The file name that will appear in AWS S3 bucket.
        If an object_name is not specified, the file will
        have the same name as the file_name
    
    Returns
    -------
    True or False: bool
        True if file was uploaded, else False
    """

    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = file_name

    # Upload the file
    s3_client = boto3.client('s3')
    try:
        response = s3_client.upload_file(file_name, bucket, object_name)
    except ClientError as e:
        logging.error(e)
        return False
    return True

################### Execution #####################
if __name__ == "__main__":
    # Connect to AWS account S3 buckets.
    s3 = boto3.resource('s3')
   
    # Acquire web developer job posting data from AWS S3 Bucket
    # Update the file with new job postings.
    df = daily_update_wd()
    
    # Prepare the web developer job posting data and upload it to AWS S3
    df_prepared = prepare_job_posts_indeed()
   
    # Upload the JSON file of prepared job postings to the wdpreparedjobpostings
    upload_to_S3_bucket(file_name='df_wd_tx_prepared.json')