######################## Introduction ###########################
'''
This py file prepares web developer job posts
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


def upload_file(file_name, bucket='wdpreparedjobpostings', object_name=None):
    """Upload a file to an S3 bucket

    :param file_name: File to upload
    :param bucket: Bucket to upload to
    :param object_name: S3 object name. If not specified then file_name is used
    :return: True if file was uploaded, else False
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
    # Get current date
    today = date.today()
    # Conver the datetime to string format
    today = today.strftime('%m%d%Y')
    
    # Name of file to be uploaded to S3 bucket, `dsrawjobpostings`
    file_name = f"cleaned_wd_tx_indeed_{today}.csv"
    
    # Data Prep Functions

    # Connect to AWS S3 Account and upload web developer job posts.
    s3 = boto3.resource('s3')
    upload_file(file_name=file_name)