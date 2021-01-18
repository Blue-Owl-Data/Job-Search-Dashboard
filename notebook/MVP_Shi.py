# General Libraries
import numpy as np
import pandas as pd

# Web Scraping Libraries
import requests
from bs4 import BeautifulSoup
import urllib

from tqdm.notebook import tqdm

def fisrt_page_url_indeed(job_title, location):
    '''
    This function returns a URL of a job search at Indeed.com 
    based on the job title and the location.
    '''
    # Create the base URL for a job serch at Indeed.com
    base_url = 'https://www.indeed.com/jobs?'
    # Create a dictionary to map the keys to the input parameters
    dic = {'q': job_title, 'l': location, 'sort': 'date'}
    # Convert the dictionary to a query string
    relative_url = urllib.parse.urlencode(dic)
    # Generate the full URL of the first page
    url = base_url + relative_url
    return url

def first_page_soup_indeed(job_title, location):
    '''
    This function returns a BeautifulSoup object to hold 
    the content of a request for job searching at Indeed.com
    '''
    # Generate the URL of the job search based on title and location
    url = fisrt_page_url_indeed(job_title, location)
    # Make the HTTP request
    response = requests.get(url)
    # Print the status code of the request
    print("Status code of the request: ", response.status_code)
    # Sanity check to make sure the document type is HTML
    print("Document type: ", response.text[:15])
    # Make a soup to hold the response content
    soup = BeautifulSoup(response.content, "html.parser")
    # Print out the title of the content
    print("Title of the response: ", soup.title.string)
    return soup

def extract_jobcards_indeed(job_title, location):
    '''
    '''
    # Generate the URL of the job search based on title and location
    url = url_indeed(job_title, location)
    # Make the HTTP request
    response = requests.get(url)
    # Print the status code
    print("Status Code: ", response.status_code)
    # Sanity check to make sure the document type is HTML
    print(response.text[:100])
    # Make a soup to hold the response content
    soup = BeautifulSoup(response.content, "html.parser")
    # Print out the title
    print("Soup Title: ", soup.title.string)
    
    return soup