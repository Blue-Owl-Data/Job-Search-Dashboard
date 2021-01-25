# General Libraries
import numpy as np
import pandas as pd

# Web Scraping Libraries
import requests
from bs4 import BeautifulSoup
import urllib

# Regex Library
import re

# Time-related Libraries
import time

# NLP Libraries
import nltk

########################### Acquisition #################################
def first_page_url_indeed(job_title, location):
    '''
    This function returns a URL of the 1st page of a job search at Indeed.com 
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
    This function returns a BeautifulSoup object to hold the content 
    of the first page of a request for job searching at Indeed.com
    '''
    # Generate the URL of the job search based on title and location
    url = first_page_url_indeed(job_title, location)
    # Make the HTTP request
    response = requests.get(url)
    # Print the status code of the request
    print("Status code of the request: ", response.status_code)
    # Sanity check to make sure the document type is HTML
    print("Document type: ", response.text[:15])
    # Take a break
    time.sleep(5)
    # Make a soup to hold the response content
    soup = BeautifulSoup(response.content, "html.parser")
    # Print out the title of the content
    print("Title of the response: ", soup.title.string)
    return soup

# def urls_indeed(job_title, location):
#     '''
#     This function returns all the URLs in a job searching result.
#     Prerequisite functions: 
#     - first_page_url_indeed
#     - first_page_soup_indeed
#     - num_jobs_indeed
#     '''
#     # Create a variable urls to hold the URLs of all pages
#     urls = []
#     # Generate the URL of the first page
#     first_page_url = first_page_url_indeed(job_title, location)
#     # Append the URL of the first page
#     urls.append(first_page_url)
#     # Generate the Soup object of the first page
#     first_page_soup = first_page_soup_indeed(job_title, location)
#     # Compute the total number of jobs based on the search
#     num_jobs = num_jobs_indeed(first_page_soup) 
#     # Estimate the total number of pages based on 15 job cards each page
#     num_page = round(int(num_jobs)/15) + 1
#     # For Loop through all the pages to generate their URLs
#     for i in range(1, num_page+1):
#         dic = {'start': i*10}
#         relative_url = urllib.parse.urlencode(dic)
#         url = first_page_url + '&' + relative_url
#         urls.append(url)
#     return urls

def page_soup_indeed(url):
    '''
    This function returns a BeautifulSoup object to hold the content 
    of a page for a job searching results at Indeed.com
    '''
    # Make the HTTP request
    response = requests.get(url)
    # Print the status code of the request
    print("Status code of the request: ", response.status_code)
    # Sanity check to make sure the document type is HTML
    print("Document type: ", response.text[:15])
    # Take a break
    time.sleep(5)
    # Make a soup to hold the response content
    soup = BeautifulSoup(response.content, "html.parser")
    # Print out the title of the content
    print("Title of the response: ", soup.title.string)
    return soup

def page_num_indeed(url):
    '''
    This function returns the page number of the job searching results. 
    '''
    # Create a Soup object based on the url
    soup = page_soup_indeed(url)
    # Find out the section contains total number of jobs  
    div = soup.find('div', id='searchCountPages')
    # Extract the number
    page_num = re.findall(r'(\d+)', div.text)[0]
    return page_num

def num_jobs_indeed(url):
    '''
    This function returns the total number of the jobs in the searching result.
    '''
    # Create a Soup object based on the url
    soup = page_soup_indeed(url)
    # Find out the section contains total number of jobs  
    div = soup.find('div', id='searchCountPages')
    # Extract the number
    num_jobs = re.findall(r'(\d+)', div.text)[1]
    return num_jobs

def job_cards_indeed(soup):
    '''
    This function accepts the Soup object of a Indeed page 
    return an iterator containing the all the job cards in this page.
    '''
    # Find the appropriate tag that contains all of the job listings in this page
    tag = soup.find('td', id="resultsCol")
    # Extract all job cards
    job_cards = tag.find_all('div', class_='jobsearch-SerpJobCard')
    return job_cards

def job_titles_indeed(job_cards):
    '''
    This function extracts the job titles from a set of job cards. 
    '''
    # Create a list to hold the job titles
    titles = []
    # For loop through the job cards to pull the titles
    for job in job_cards:
        title = job.find('h2', class_='title')
        title = title.text.strip()
        titles.append(title)
    return titles

def company_names_indeed(job_cards):
    '''
    This function extracts the company names from a set of job cards.
    '''
    # Create a list to hold the company names
    names = []
    # For loop through the job cards to pull the company names
    for job in job_cards:
        name = job.find('span', class_='company')
        name = name.text.strip()
        names.append(name)
    return names

def post_ages_indeed(job_cards):
    '''
    This function pulls the post ages from a set of job cards.
    '''
    # Create a list to hold the post ages
    ages = []
    # For loop through the job cards to pull the post ages
    for job in job_cards:
        age = job.find('span', class_='date')
        age = age.text.strip()
        ages.append(age)
    return ages

def acuqire_indeed_job_description(url):
    '''
    This function accepts the URL of a job posting and pull its description.
    '''
    # Make the HTTP request
    request = requests.get(url)
    print("Status Code: ", request.status_code)
    # Take a break
    time.sleep(5)
    # Make a soup variable holding the response content
    soup = BeautifulSoup(request.content, "html.parser")
    if soup == None:
        description = 'error'
    else:
        # Print the page's title
        print(soup.title.string)
        # Find the section that contains job description
        description = soup.find('div', id="jobDescriptionText")
        if description == None:
            description = 'error'
        else:
            description = description.text
    return description

def job_links_and_contents_indeed(job_cards):
    '''
    This function pulls the job links and descriptions from a set of job cards.
    '''
    # Create a list to hold the links and descriptions
    links = []
    descriptions = []
    # For loop through the job cards to pull the links and descriptions
    for job in job_cards:
        link = job.find('a')['href']
        link = 'https://www.indeed.com' + link
        link = link.replace(';', '&')
        description = acuqire_indeed_job_description(link)
        links.append(link)
        descriptions.append(description)
    return links, descriptions

def job_locations_indeed(job_cards):
    '''
    This function pulls the job locations from a set of job cards.
    '''
    # Create a list to hold the locations
    locations = []
    # For loop through the job cards to pull the locations
    for job in job_cards:
        location = job.find('div', class_='location accessible-contrast-color-location')
        if location == None:
            location = job.find('span', class_='location accessible-contrast-color-location')
        location = location.text.strip()
        locations.append(location)
    return locations

def company_rating_indeed(job_cards):
    '''
    This function pulls the company rating from a set of job cards.
    If the rating is unavailable, it will be marked as 'missing'.
    '''
    # Create a list to hold the locations
    ratings = []
    # For loop through the job cards to pull the locations
    for job in job_cards:
        rating = job.find('span', class_='ratingsContent')
        if rating == None:
            ratings.append('missing')
            continue
        rating = rating.text.strip()
        ratings.append(rating)
    return ratings

def acquire_page_indeed(url):
    '''
    This function accepts a job search URL and returns a pandas dataframe 
    containing job title, location, company, company rating, post age and description. 
    '''
    # Create a Soup object based on the url
    soup = page_soup_indeed(url)
    # Pull the job cards
    job_cards = job_cards_indeed(soup)
    # Pull the job titles
    titles = job_titles_indeed(job_cards)   
    # Pull the names of the companies
    companies = company_names_indeed(job_cards)
    # Pull the post ages
    ages = post_ages_indeed(job_cards)
    # Pull the job locations
    locations = job_locations_indeed(job_cards)
    # Pull the company ratings
    ratings = company_rating_indeed(job_cards)
    # Pull the hyperlinks and job description
    links, descriptions = job_links_and_contents_indeed(job_cards)    
    # Create a dataframe
    d = {'title': titles,
         'locations': locations,
         'company': companies, 
         'company_rating': ratings,
         'post_age': ages, 
         'job_link': links, 
         'job_description': descriptions}
    df = pd.DataFrame(d)
    return df

def jobs_indeed(job_title, location):
    '''
    This function accepts the job title and location and return 
    the job information pull from Indeed.com.
    '''
    # Generate the urls based on job title and location (state)
    url = first_page_url = first_page_url_indeed(job_title, location)
    # Print the total number of jobs
    print(f"Total number of {job_title} in {location}: ", num_jobs_indeed(url))
    # Set up an counter
    counter = 1
    # Create an empty dataframe to hold the job information
    df_jobs = pd.DataFrame(columns = ['title', 'locations', 'company', 'company_rating', 
                                      'post_age','job_link', 'job_description'])
    # Pull the page number
    page_num = int(page_num_indeed(url))
    # Set up an checker
    keep_going = (counter == page_num)   
    # For loop through the urls to pull job information
    while keep_going and page_num <=40:
        df = acquire_page_indeed(url)
        print("--------------------------------")
        print("Page: ", page_num)
        print("--------------------------------")
        df_jobs = df_jobs.append(df, ignore_index=True)
        time.sleep(180)
        dic = {'start': page_num*10}
        relative_url = urllib.parse.urlencode(dic)
        url = first_page_url + '&' + relative_url
        counter = counter + 1
        page_num = int(page_num_indeed(url))
        keep_going = (counter == page_num)
    # Print the total number of jobs
    print(f"Total number of {job_title} positions in {location}: ", df_jobs.shape[0])
    return df_jobs

########################### Exploration #################################
def words_variables(df, companies):
    '''
    This function accepts the dataframe containing cleaned job description and 
    a list of company names and return a dictionary in which the values are the words 
    that appear in the job description. 
    '''
    # Create the words that appear all the job descritipons
    all_words = ' '.join(df.clean)
    # Create a dictionary to hold the variable all_words
    d_words = {'all': all_words}
    # For loop the companies and create the words that appear in their job descriptions
    for company in companies:
        mask = (df.company == company)
        s_company = df[mask].clean
        words = ' '.join(s_company)
        d_words[company] = words
    return d_words

def word_frequency(d_words):
    '''
    This function accept the dictionary created by function words_variables
    and return the word frequency in the job description. 
    '''
    # Read the company names from the dictionary
    companies = d_words.keys()
    # Create a dataframe to hold the word frequency
    word_counts = pd.DataFrame()
    # For loop through the companies and generate the word frequency in their job descriptions
    for company in companies:
        freq = pd.Series(d_words[company].split()).value_counts()
        word_counts = pd.concat([word_counts, freq], axis=1, sort=True)
    word_counts.columns = companies
    word_counts = word_counts.fillna(0).apply(lambda s: s.astype(int))
    word_counts.sort_values(by='all', ascending=False, inplace=True)
    return word_counts

def bigrams_frequency(d_words):
    '''
    This function accept the dictionary created by function words_variables
    and return the bigrams frequency in the job description. 
    '''
    # Read the company names from the dictionary
    companies = d_words.keys()
    # Create a dataframe to hold the word frequency
    bigrams_counts = pd.DataFrame()
    # For loop through the companies and generate the word frequency in their job descriptions
    for company in companies:
        freq = pd.Series(list(nltk.ngrams(d_words[company].split(), 2))).value_counts()
        bigrams_counts = pd.concat([bigrams_counts, freq], axis=1, sort=True)
    bigrams_counts.columns = companies
    bigrams_counts = bigrams_counts.fillna(0).apply(lambda s: s.astype(int))
    bigrams_counts.sort_values(by='all', ascending=False, inplace=True)
    return bigrams_counts

def trigrams_frequency(d_words):
    '''
    This function accept the dictionary created by function words_variables
    and return the trigrams frequency in the job description. 
    '''
    # Read the company names from the dictionary
    companies = d_words.keys()
    # Create a dataframe to hold the word frequency
    trigrams_counts = pd.DataFrame()
    # For loop through the companies and generate the word frequency in their job descriptions
    for company in companies:
        freq = pd.Series(list(nltk.ngrams(d_words[company].split(), 3))).value_counts()
        trigrams_counts = pd.concat([trigrams_counts, freq], axis=1, sort=True)
    trigrams_counts.columns = companies
    trigrams_counts = trigrams_counts.fillna(0).apply(lambda s: s.astype(int))
    trigrams_counts.sort_values(by='all', ascending=False, inplace=True)
    return trigrams_counts

def remove_duplicates(df):
    '''
    This function removes the duplicates in the dataframe
    '''
    # Define the columns for identifying duplicates
    columns = ['title', 'locations', 'company', 'job_link', 'job_description']
    # Drop the duplicates except for the last occurrence
    df.drop_duplicates(subset=columns, keep='last', inplace=True, ignore_index=True)
    return df