######################## Introduction ###########################
'''
This py file prepares data scientist job posts
for data exploration.
'''
###################### Import Libraries #########################
'''General Libraries'''
import numpy as np
import pandas as pd

'''AWS S3 Libraries'''
import logging
import boto3
from botocore.exceptions import ClientError

'''Regex Library'''
import re

'''Time-related Libraries'''
import time
from datetime import date

import MVP_Bojado

###################### Build Helper Functions ####################
def compute_post_date(df):
    '''
    This function computes the date of the job post based on post age
    and set the date as the index of the dataframe.
    '''
    # Create an empty list to hold the post date
    post_date = []
    # For loop the column post_age and convert the values to date
    for age in df.post_age:
        if age == 'Just posted':
            date = datetime.date.today()
            post_date.append(date)
        elif age == 'Today':
            date = datetime.date.today()
            post_date.append(date)
        else:
            # Extract the number
            num = re.findall(r'(\d+)', age)[0]
            # Cast the string number to integer
            num = int(num)
            # Convert the integer to timedelta object
            num = datetime.timedelta(days=num)
            # Compute post date        
            date = datetime.date.today()
            date = date - num
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
    columns = ['title', 'locations', 'company', 'job_link', 'job_description']
    # Drop the duplicates except for the last occurrence
    df.drop_duplicates(subset=columns, inplace=True, keep='last')
    return df


def daily_update_ds():
    '''
    This function updates job posts of data scientist in TX by adding the daily acquring
    of data scientist job posts in TX. 
    '''
    # Read the job posts of data scientist in TX
    download_from_S3_bucket()
    df_ds_tx = pd.read_csv("df_ds_tx.csv")
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
    file_name = "ds_tx_indeed_" + today + ".csv"
    
    df = pd.read_csv(file_name)
    
    # Add the daily update
    df = compute_post_date(df)
    df_ds_tx = pd.concat([df_ds_tx, df]).sort_index(ascending=False)
    # Remove the duplicates
    df_ds_tx = remove_duplicates(df_ds_tx)
    # Save as csv file
    df_ds_tx.to_csv("df_ds_tx.csv")
    # Upload the updated data science job postings to S3
    upload_to_S3_bucket("df_ds_tx.csv", bucket='dsrawjobpostings', object_name=None)
    # Print the new jobs posted today
    num_new_jobs = df_ds_tx.shape[0] - num_jobs
    print("New Jobs Posted Today: ", num_new_jobs)
    return df_ds_tx


def prepare_job_posts_indeed():
    '''
    The function reads the csv file of job posts and returns a cleaned dataframe
    ready for exploration.
    '''
    # Read the job posts of data scientist in TX
    df = pd.read_csv("df_ds_tx.csv")
    # Conver the string date to datetime object
    df.date = pd.to_datetime(df.date)
    # Set the date as the index and sort the dataframe in descending order
    df = df.set_index('date').sort_index(ascending=False)
    # Drop the column post_age
    df = df.drop(columns='post_age')
    # Clean the text in the job description
    df = MVP_Bojado.prep_job_description_data(df, 'job_description')
    # Save a JSON version of the prepared data
    df.to_json('df_ds_tx_prepared.json')
    
    return df

###################### Download and Upload Job Postings to AWS S3 ####################
def list_bucket_files(bucket_name='dsrawjobpostings'):
    '''
    This function lists all files inside of a bucket
    
    
    Parameters
    ----------
    bucket_name : str, default='dsrawjobpostings'
        The name of the AWS S3 bucket that contains the
        raw data science job postings.
        
    Returns
    -------
    files : list
        A list of file names contained within the specified
        AWS S3 bucket.
    '''
    # Connect to AWS Account and access the available S3 buckets.
    s3 = boto3.resource('s3')

    # Select the raw data science job posting bucket.
    ds_job_bucket = s3.Bucket(bucket_name)
    
    # Iterate through each file in the bucket and display the name
    files = []
    for page in ds_job_bucket.objects.pages():
    for obj in page:
        print(obj.key)
        # Append the file name to the list of file names.
        files.append(obj.key)
    # Return the list of file names available in the bucket..
    return files


def download_from_S3_bucket():
    '''
    This function downloads raw data science job postings from an AWS S3 bucket.
    The data is stored as a CSV file in the local directory.
    
    Parameters
    ---------
    None
    
    Returns
    -------
    None
    '''
    s3 = boto3.client('s3')
    s3.download_file('dsrawjobpostings', 'df_ds_tx.csv', 'df_ds_tx.csv')
    

def upload_to_S3_bucket(file_name, bucket='dspreparedjobpostings', object_name=None):
    """
    Upload a file to an S3 bucket
    
    ***Prepared data files must be in JSON format***

    Parameters
    ----------
    file_name: str
        Name of the file to upload.
    
    bucket: str, default="dspreparedjobpostings"
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
    # Get current date
    today = date.today()
    # Conver the datetime to string format
    today = today.strftime('%m%d%Y')
    
    # Name of the file that will be uploaded to the S3 bucket: `dsrpreparedjobpostings`
    # Save as a JSON file for the Front and Backend Devs.
    s3_name = f"ds_tx_indeed_{today}.json"
   
    # Acquire data scientist job posting data from AWS S3 Bucket
    # Update the file with new job postings.
    df = daily_update_ds()
    
    # Prepare the data science job posting data and upload it to AWS S3
    df_prepared = prepare_job_posts_indeed()
   
    # Upload the JSON file of prepared job postings to the dspreparedjobpostings
    upload_to_S3_bucket(file_name='df_ds_tx_prepared.json', object_name=s3_name)