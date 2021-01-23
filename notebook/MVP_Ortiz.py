import pandas as pd
import requests
import os

###################### REST API Functions ######################
def base_url():
    '''
    Returns base url to acquire LinkedIn job posts.
    
    Parameters
    ----------
    None
    
    Returns
    -------
    string url to acquire LinkedIn data.
    '''
    return  "api.linkedin.com/v2/"


def response_endpoint(endpoint):
    '''
    Accepts endpoint to a specified H-E-B dataset.
    
    Returns 
    Parameters
    ----------
    endpoint : str
        A possible path of "https://python.zach.lol"
        
        Example
        -------
        endpoint = "/documentation"
        base_url = "https://python.zach.lol"
        
    Returns
    -------
    requests.models.Response object
    '''
    get_request = requests.get(base_url() + endpoint)
    return get_request


def max_page(url):
    '''
    Accepts a requests.models.Response object
    
    Return the maximum page for a specific endpoint
    Parameters
    ----------
    url : requests.models.Response
        A response from an endpoint using REST API
    
    Returns
    -------
    integer value
    '''
    return url.json()['payload']['max_page']


def page_iterator(data, data_path, stop_page):
    '''
    Accepts an endpoint name, path to endpoint, and number of pages to acquire.
    
    Return a specific H-E-B dataset as a pandas DataFrame.
    Parameters
    ----------
    data : str
        The name of an endpoint to retrieve H-E-B data
    
    data_path : str
        The path to the specified endpoint
    
    stop_page : int
        The page number to stop on - inclusive.
    
    Returns
    -------
    pandas DataFrame
    '''
    df = pd.DataFrame()
    
    for page in range(1, stop_page+1):
        response = requests.get(base_url() + data_path + '?page=' + str(page))
        df = df.append(response.json()['payload'][f'{data}'])

    return df


def check_local_cache(data):
    '''
    Accepts an endpoint from https://python.zach.lol and checks to see if a local
    cached version of the data exists
    
    Returns endpoint data as a pandas DataFrame if a local cache exists
    Returns False if a local cache does not exist.
    
    Parameters
    ----------
    data : str
        H-E-B datasets
        --------------
        'stores': Checks for a cached file of the 'stores' dataset
        'items' : Checks for a cached file of the 'items' dataset
        'sales' : Checks for a cached file of the 'sales' dataset
        Germany dataset
        ---------------
        'power' : Open Power Systems Data for Germany
        
    Returns
    -------
    Return cached file as a pandas DataFrame if : os.path.isfile(file_name) == True
    Return False otherwise
    '''
    file_name = f'{data}.csv'
    
    if os.path.isfile(file_name):
        return pd.read_csv(file_name, index_col=False)
    else:
        return False