# General Libraries
import numpy as np
import pandas as pd

# Web Scraping Libraries
import requests
from bs4 import BeautifulSoup
import urllib

from tqdm.notebook import tqdm

def url_indeed(job_title, location):
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
    # Generate the full URL
    url = base_url + relative_url
    return url