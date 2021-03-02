import pandas as pd
import requests
import os

# Geospatial Libraries
import geopandas as gpd
import geopy
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter
import folium

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

###################### Geospacial Functions ######################
def get_geodata(df, credentials="Blue-Owl-Data"):
    '''
    This function accepts a dataframe of job postings that has
    the column names: 'city_state' where the values are Example: "Austin, Texas".
    
    Returns a UNIQUE dataframe of each (city, state):
    'city_state' : City and state where the job is located. Example: "Austin, Texas"
    'latitude'   : Latitude coordinate of the city
    'longitude'  : Longitude coordinate of the city
    
    Dependency Requirements:
    import geopy
    from geopy.geocoders import Nominatim
    from geopy.extra.rate_limiter import RateLimiter
    
    $ pip install geopandas
    
    
    Parameters
    ----------
    df : pandas.core.DataFrame()
    
    credentials : str, default "Blue-Owl-Data"
        Name of an application needed to request data from
        OpenStreetMap.
        
    Returns
    -------
    df_coordinates : pandas.core.DataFrame() 
    '''
    # Create a User-Agent name to use geopy
    geolocator = Nominatim(user_agent=credentials)
    # Wrap the goelocator.geocode function in a RateLimiter function to
    # pause between API calls.
    geocode = RateLimiter(geolocator.geocode, min_delay_seconds=1/20)

    # Create a Series of unique (city, state) combinations.
    city_states = df.city_state.unique()

    # Loop through each (city, state) and create a geocode object
    geodata = [geocode(location, language="en") for location in city_states]

    # Loop through each geocode object and extract the latitude and longitude.
    latitudes =  [city.latitude for city in geodata]
    longitudes =  [city.longitude for city in geodata]
    
    # Create a dataframe to store the geodata of each location
    df_coordinates = pd.DataFrame({'city_state' : city_states,
                                   'latitude' : latitudes,
                                   'longitude' : longitudes})
    
    return df_coordinates
    
    
def add_coordinates(df):
    '''
    This function accepts a dataframe of job postings that have
    the column names: 'city' and 'state'.
    
    Returns the original dataframe with new columns:
    'city_state' : City and state where the job is located. Example: "Austin, Texas"
    'latitude'   : Latitude coordinate of the city
    'longitude'  : Longitude coordinate of the city
    
    Dependency Requirements:
    import geopy
    from geopy.geocoders import Nominatim
    from geopy.extra.rate_limiter import RateLimiter
    
    $ pip install geopandas
    
    
    Parameters
    ----------
    df : pandas.core.DataFrame()
        
    Returns
    -------
    df_updated : pandas.core.DataFrame() 
    '''
    # Create a new column called 'city_state' that contains the
    # city, state as a string. Example: "Dallas, Texas"
    df = df.assign(
        # Cast 'city' explicitly to a string datatype. There are
        # a few job postings without a city, where city == 0
        city_state = df['city'].astype('str') + ', ' + df['state']
    )
    
    # Create a dataframe to store geodata of each location
    df_geodata = get_geodata(df)
    df_updated = df.merge(df_geodata, how='left', on='city_state')
    return df_updated