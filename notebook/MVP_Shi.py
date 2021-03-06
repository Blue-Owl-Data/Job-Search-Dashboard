# General Libraries
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

# Web Scraping Libraries
import requests
from bs4 import BeautifulSoup
import urllib

# Regex Library
import re

# Time-related Libraries
import time
import datetime

# NLP Libraries
import nltk

# AWS
import boto3

# Environment File
import env_Shi

# Import Self Defined Functions
import MVP_Bojado

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
        if title == None:
            title = 'error'
        else: 
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
        if name == None:
            name = 'error'
        else: 
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
        if age == None:
            age = 'error'
        else: 
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
         'location': locations,
         'company': companies, 
         'company_rating': ratings,
         'post_age': ages, 
         'job_link': links, 
         'job_description': descriptions}
    df = pd.DataFrame(d)
    return df

def jobs_indeed(job_title, location, max_page=35):
    '''
    This function accepts the job title and location and return the job information (35 pages by default) 
    pulled from Indeed.com.
    '''
    # Generate the urls based on job title and location (state)
    url = first_page_url = first_page_url_indeed(job_title, location)
    # Set up an counter
    counter = 1
    # Create an empty dataframe to hold the job information
    df_jobs = pd.DataFrame(columns = ['title', 'location', 'company', 'company_rating', 
                                      'post_age','job_link', 'job_description'])
    # Pull the page number
    page_num = int(page_num_indeed(url))
    # Set up an checker
    keep_going = (counter == page_num)   
    # For loop through the urls to pull job information
    while keep_going and page_num <= max_page:
        df = acquire_page_indeed(url)
        print("--------------------------------")
        print("Page: ", page_num)
        print("--------------------------------")
        df_jobs = df_jobs.append(df, ignore_index=True)
        df_jobs.to_csv("df_jobs_backup.csv")
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

########################### Preparation #################################
def remove_duplicates(df):
    '''
    This function removes the duplicates in the dataframe based on title, location, 
    company, job_link, and job_description
    '''
    # Define the columns for identifying duplicates
    columns = ['title', 'location', 'company', 'job_link', 'job_description']
    # Drop the duplicates except for the last occurrence
    df.drop_duplicates(subset=columns, inplace=True, keep='last')
    return df

def compute_post_date(df):
    '''
    This function computes the date of a job posting based on its post age
    and set the date as the index of the dataframe.
    '''
    # Create an empty list to hold the post date
    post_date = []
    # For loop the column post_age and convert the values to date
    for age in df.post_age:
        if age == 'Just posted' or age == 'Today':
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

def transform_old_file(df, date_string):
    '''
    This function accepts old daily job posts and convert the post age to post date. 
    '''
    # Change column name to location
    df = df.rename(columns={'location': 'location'})
    # Create an empty list to hold the post date
    post_date = []
    # For loop the column post_age and convert the values to date
    for age in df.post_age:
        if age == 'Just posted':
            date = datetime.date.fromisoformat(date_string)
            post_date.append(date)
        elif age == 'Today':
            date = datetime.date.fromisoformat(date_string)
            post_date.append(date)
        else:
            # Extract the number
            num = re.findall(r'(\d+)', age)[0]
            # Cast the string number to integer
            num = int(num)
            # Convert the integer to timedelta object
            num = datetime.timedelta(days=num)
            # Compute post date        
            date = datetime.date.fromisoformat(date_string)
            date = date - num
            post_date.append(date)
    # Add post date as new column
    df['date'] = post_date
    # Set the column post_date as the index and sort the values
    df = df.set_index('date').sort_index(ascending=False)
    return df

def clean_job_title(title):
    '''
    This function removes the "\nnew" and "..." in the job title.
    '''
    title = title.split(sep="\nnew")[0]
    title = title.split(sep="...")[0]
    return title

def daily_update(df_new):
    '''
    This function updates and prepares the job posts by adding daily new job postings 
    and save as JSON file. 
    '''
    # Load the job posts based on the inputted job title
    database = env_Shi.database
    print("Enter the INITIALS of the job title:")
    initials = input()
    df = pd.read_csv(f"{database}df_{initials}_tx_backup.csv")
    num_jobs = df.shape[0]
    # Convert the date column to datetime type
    df.date = pd.to_datetime(df.date)
    # Set the date column as the index and sort the index
    df = df.set_index('date').sort_index(ascending=False)
    
    # Add the new job postings
    df_new = compute_post_date(df_new)
    df = pd.concat([df, df_new]).sort_index(ascending=False)
    
    # Remove the duplicates
    df = remove_duplicates(df)
    
    # Back up the jop postings as a csv file before data cleaning phase I
    df.to_csv(f"{database}df_{initials}_tx_backup.csv")
    num_new_jobs = df.shape[0] - num_jobs
    print("New Jobs of Posted Today: ", num_new_jobs)
    
    # Re-Load the dataset
    df = pd.read_csv(f"{database}df_{initials}_tx_backup.csv")
    
    # Clean the location data: break the location to city, state, and zipcode
    location = df.location.str.split(', ', expand=True)
    location.columns = ['city', 'zipcode']
    # Imputation
    location.city = location.city.apply(lambda i: 'Remote' if i == 'United States' else i)
    location.city = location.city.apply(lambda i: 'Remote' if i == 'Texas' else i)
    location.zipcode = location.zipcode.apply(lambda i: 0 if re.findall(r"(\d+)", str(i)) == [] 
                                              else re.findall(r"(\d+)", str(i))[0])
    df['city'] = location.city
    df['state'] = 'TX'
    df['zipcode'] = location.zipcode
    
    # Replace the missing values in the company rating with the mean
    df.company_rating = df.company_rating.replace('missing', np.NaN).astype(float)
    mean_rating = df.company_rating.mean()
    df.company_rating.fillna(mean_rating, inplace=True)
    df.company_rating = df.company_rating.round(2)
    
    # Clean the text in the job description
    df = MVP_Bojado.prep_job_description_data(df, 'job_description')
    
    # Clean the job title
    df.title = df.title.apply(clean_job_title)
    
    # Drop the redundant columns post_age and location
    redundant_cols = ['post_age', 'location', 'tokenized', 'stemmed', 'lemmatized']
    df = df.drop(columns=redundant_cols)
    
    # Alther the data type of zipcode
    df.zipcode = df.zipcode.apply(lambda i: int(i))
    
    # Save a JSON version of the prepared data
    df.to_json(f"{database}df_{initials}_tx_prepared_backup.json", orient='records')
    return df

########################### Exploration #################################
def read_job_postings_json():
    '''
    This function reads the JSON file of prepared job postings into a pandas dataframe 
    based on a job title and set the date as the index.
    '''
    # Load the file path of the local database
    database = env_Shi.database
    # Create the file name
    print("Enter the INITIALS of the job title:")
    initials = input()
    file_name = 'df_' + initials + '_tx_prepared_backup.json'
    # Read the JSON file into a pandas dataframe
    df = pd.read_json(f'{database}{file_name}')
    # Print the numbr of job posts
    print("Number of Job Postings: ", df.shape[0])
    # Convert the string date to datetime
    df.date = pd.to_datetime(df.date)
    # Set the date as the index and sort the dataframe
    df = df.set_index('date').sort_index(ascending=False)
    return df

def words_variables_v1(df):
    '''
    This function accepts the dataframe with cleaned job description 
    and return a dictionary in which the values are the words that 
    appear in the job description. 
    '''
    # Create the words that appear all the job descritipons
    all_words = ' '.join(df.clean)
    # Create a dictionary to hold the variable all_words
    d_words = {'frequency': all_words}
    return d_words

def words_variables_v2(df, companies):
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

def word_frequency_v1(d_words):
    '''
    This function accept the dictionary created by function words_variables_v1
    and return the word frequency in the job description. 
    '''
    # Create a dataframe to hold the word frequency
    word_counts = pd.DataFrame()
    # Compute the words frequency
    freq = pd.Series(d_words['frequency'].split()).value_counts()
    # Add the `freq` seires to `word_counts` dataframe
    word_counts = pd.concat([word_counts, freq], axis=1, sort=True)
    # Rename the coumns
    word_counts.columns = d_words.keys()
    # Sort the dataframe by the values in column `frequency`
    word_counts.sort_values(by='frequency', ascending=False, inplace=True)
    return word_counts

def word_frequency_v2(d_words):
    '''
    This function accept the dictionary created by function words_variables_v2
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

def bigrams_frequency_v1(d_words):
    '''
    This function accept the dictionary created by function words_variables_v1
    and return the word frequency in the job description. 
    '''
    # Create a dataframe to hold the frequency of bigrams
    word_counts = pd.DataFrame()
    # Compute the words frequency
    freq = pd.Series(list(nltk.ngrams(d_words['frequency'].split(), 2))).value_counts()
    # Add the `freq` seires to `word_counts` dataframe
    word_counts = pd.concat([word_counts, freq], axis=1, sort=True)
    # Rename the coumns
    word_counts.columns = d_words.keys()
    # Sort the dataframe by the values in column `frequency`
    word_counts.sort_values(by='frequency', ascending=False, inplace=True)
    return word_counts

def bigrams_frequency_v2(d_words):
    '''
    This function accept the dictionary created by function words_variables_v2
    and return the bigrams frequency in the job description. 
    '''
    # Read the company names from the dictionary
    companies = d_words.keys()
    # Create a dataframe to hold the frequency of bigrams
    bigrams_counts = pd.DataFrame()
    # For loop through the companies and generate the word frequency in their job descriptions
    for company in companies:
        freq = pd.Series(list(nltk.ngrams(d_words[company].split(), 2))).value_counts()
        bigrams_counts = pd.concat([bigrams_counts, freq], axis=1, sort=True)
    bigrams_counts.columns = companies
    bigrams_counts = bigrams_counts.fillna(0).apply(lambda s: s.astype(int))
    bigrams_counts.sort_values(by='all', ascending=False, inplace=True)
    return bigrams_counts

def trigrams_frequency_v1(d_words):
    '''
    This function accept the dictionary created by function words_variables_v1
    and return the word frequency in the job description. 
    '''
    # Create a dataframe to hold the frequency of trigrams
    word_counts = pd.DataFrame()
    # Compute the words frequency
    freq = pd.Series(list(nltk.ngrams(d_words['frequency'].split(), 3))).value_counts()
    # Add the `freq` seires to `word_counts` dataframe
    word_counts = pd.concat([word_counts, freq], axis=1, sort=True)
    # Rename the coumns
    word_counts.columns = d_words.keys()
    # Sort the dataframe by the values in column `frequency`
    word_counts.sort_values(by='frequency', ascending=False, inplace=True)
    return word_counts

def trigrams_frequency_v2(d_words):
    '''
    This function accept the dictionary created by function words_variables_v2
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

def everygram_frequency_v1(d_words, max_len=3):
    '''
    This function accetps the dictionary produced by the function `words_variables_v1` and 
    return mono-, bi-, and tri-grams along with their frequency. 
    '''
    # Generate mono-, bi-, and tri-grams
    grams = nltk.everygrams(d_words['frequency'].split(), max_len=max_len) # dtype of grams: <class 'genertor'>
    # Convert to a list of tuples
    grams = list(grams)
    # Create an empty list to hold mono-, bi-, and tri-grams
    everygram = []
    # For loop the list of tuples and convert the grams to strings
    for gram in grams:
        str_gram = gram[0]
        for i in gram[1:]:
            str_gram = str_gram + ' ' + i
        everygram.append(str_gram)
    # Compute the frequency of the everygrams
    everygram = pd.Series(everygram).value_counts()
    return everygram

def top_skills_v1(df, k, library, library_type):
    '''
    This function accepts a positive integer k and a skillset library and 
    returns a dataframe containing the top k skills needed for data scientist positions.
    '''
    # Create a string of all words that appear in the job description
    dic = words_variables_v1(df)
    # Compute the words frequency
    everygram_frequency = everygram_frequency_v1(dic)
    # Create a empty dataframe to hold the rank of the skills
    df_skills = pd.DataFrame()
    # For loop through the library to find out the frequency of the skills mentioned in the job description
    for skill in library:
        mask = (everygram_frequency.index == skill)
        df =  everygram_frequency[mask]
        df_skills = pd.concat([df_skills, df])
    df_skills.columns = dic.keys()
    df_skills.sort_values(by='frequency', ascending=False, inplace=True)
    # Reset the index
    df_skills.reset_index(inplace=True)
    # Rename the column name
    df_skills.rename(columns={'index': f'top{k}_{library_type}_skills'}, inplace=True)
    return df_skills.head(k)

def top_skills_ds_v2(company, k):
    '''
    This function accepts a company name and a positive integer k and 
    returns a dataframe containing the top k skills needed in that company 
    for data scientist positions.
    '''
    # Import the file path
    database = env_Shi.database
    # Load the prepared dataframe with job search results
    df = pd.read_csv(f"{database}df_tx_ds.csv", index_col=0)
    # Create a string of all words that appear in the job description
    dic = words_variables_v2(df, company)
    # Compute the words frequency
    df_word_frequency = word_frequency_v2(dic)
    # Define a library that has a complete sillset for data scientist
    library = ['python', 'r', 'sql', 'tableau', 'scikitlearn', 'tensorflow', 'pytorch', 'aws', 'hadoop', 'hive', 
        'impala', 'matlab', 'model', 'algorithm', 'storytelling', 'statistic', 'etl', 'exploration', 'extraction', 
        'sharepoint', 'dashboard']
    # Create a empty dataframe to hold the rank of the skills
    df_skills = pd.DataFrame()
    # For loop through the library to find out the frequency of the skills mentioned in the job description
    for skill in library:
        mask = (df_word_frequency.index == skill)
        df = df_word_frequency[mask]
        df_skills = pd.concat([df_skills, df])
    df_skills.sort_values(by=company, ascending=False, inplace=True)
    return df_skills.head(k)

def top_skill_frequency(df, df_top):
    '''
    This function accepts the dataframe of the prepared job postings and the top k skills and provides
    three options: 
    - to save the frequencies of the top skills over time as a JSON file and upload to AWS.
    - to return a dataframe containing only the frequecies of the top skills over time.
    - to add the frequencies of the top skills over time to the original dataframe
    - to save the merged dataframe as JSON file in the database. 
    '''
    # Reminder
    print("Please insure the date is in the right format")
    
    # Confirm the library type
    print("Confirm TYPE of library: tech or soft or general")
    library_type = input()

    # Confirm the job title
    print("Enter the INITIALS of the job title:")
    initials = input()
    
    # Create a list of the top k skills
    skill_list = df_top.iloc[:, 0].to_list()
    
    # Create an empty dictionary to hold the frequency of the skill in each observation
    dic_frequency = {}    
    # Loop through the list of skills to compute its frequency in each observation
    for skill in skill_list:
        list_frequency = []
        for string in df.clean.values:
            matches = re.findall(f" {skill} ", string)
            frequency = len(matches)
            list_frequency.append(frequency)
        dic_frequency[skill]=list_frequency
        
    # Convert the dictionary into the dataframe and set the index the same as df
    df_frequency = pd.DataFrame(dic_frequency, index=df.index)
    
    # Save as JSON file and upload to AWS
    print("Do you want to save the dataframe as JSON and upload to AWS? (Y/N)")
    save_option = input()
    if save_option == "Y" or save_option == 'y':
        df_freq_copy = df_frequency.reset_index()
        df_freq_copy.date = df_freq_copy.date.apply(lambda i: i.strftime("%Y-%m-%d"))
        database = env_Shi.database
        file_name = f"{initials}_top_{library_type}_ts.json"
        df_freq_copy.to_json(f"{database}{file_name}", orient="records")
        s3 = boto3.resource("s3")
        s3.Bucket("additionaljobinfo").upload_file(f"{database}{file_name}", file_name)
    elif save_option == "N" or save_option == 'n':
        print("You can manually save it by yourself")
        
    # Merge two dataframe together
    print("Do you want to merge the dataframes? (Y/N)")
    merge_option = input()
    if merge_option == "Y" or merge_option == 'y':
        df_frequency = pd.concat([df, df_frequency], axis=1)
        
        # Save the merged dataframe as JSON
        print("Do you want to save the merged dataframe? (Y/N)")
        save_option = input()
        if save_option == 'Y' or save_option == 'y':
            df_freq_copy = df_frequency.reset_index()
            df_freq_copy.date = df_freq_copy.date.apply(lambda i: i.strftime("%Y-%m-%d"))
            database = env_Shi.database
            file_name = f"df_{initials}_tx_top_{library_type}_ts.json"
            df_freq_copy.to_json(f"{database}{file_name}", orient="records")
        elif save_option == 'N' or save_option == 'n':
            print("You can manually save it by yourself")

    elif merge_option == "N" or merge_option == 'n':
        print("You can manually merge it by yourself")
    
    return df_frequency

def plot_top_skill_ts(df, df_top):
    '''
    This function accetps the dataframe of preapred job postings with the frequencies of the skills
    and plot how popular each skill changes over time. 
    '''
    # Set up the size of the plot
    plt.figure(figsize=(11, 8))
    # Create a list of the top skills
    skill_list = df_top.iloc[:, 0].to_list()
    # Resample the dataset by week and plot the mean of the frequency of each skill per job posting
    for skill in skill_list:
        df.resample('W')[skill].mean().plot(label=f'{skill} Weekly')
    
    # Name the plot
    plt.title("How Popular the Top 5 Skills Are Over Time", fontweight='bold')
    # Position the legend
    plt.legend(bbox_to_anchor=(1, 1))

################################################## Skill Libraries ########################################################

library_ds_tech = ['python','sql','pandas','numpy','matplotlib','scikit learn','scikitlearn','spark','hadoop',
                'aws','amazon web services','azure','microsoft word', 'microsoft excel','excel','tableau',
                'tensor flow','pytorch','hive','impala','matlab','etl','statistics','exploration',
                'extraction','data wrangling','math','machine learning','data visualization','java','js',
                'javascript','scala','r','c','c++','power bi','dashboard','linear algebra','calculus',
                'neural networks','eda','big data','frameworks','database management','testing hypotheses',
                'probability','data mining','perl','nosql','saas','git','github','natural language processing',
                'nlp', 'deep learning','agile','kanban','project management','julia','devops','google cloud',
                'pytorch','computer vision', 'deep neural networks','neural networks','amazon web services',
                'natural language processing','extract,transform,load','mysql','structured query language']

library_ds_soft = ['critical thinking','communication','problem solving','teamwork','ethics','business acumen',
                'interpersonal skills','curiosity','storytelling','adaptability','team player','collaboration',
                'time management','leadership','domain knowledge','creativity','decision making',
                'verbal communication','written communication']

library_ds_general = ['python','sql','pandas','numpy','matplotlib','scikit learn','scikitlearn','spark','hadoop',
                'aws','amazon web services','azure','microsoft word', 'microsoft excel','excel','tableau',
                'tensor flow','pytorch','hive','impala','matlab','etl','statistics','exploration',
                'extraction','data wrangling','math','machine learning','data visualization','java','js',
                'javascript','scala','r','c','c++','power bi','dashboard','linear algebra','calculus',
                'neural networks','eda','big data','frameworks','database management','testing hypotheses',
                'probability','data mining','perl','nosql','saas','git','github','natural language processing',
                'nlp', 'deep learning','agile','kanban','project management','julia','devops','google cloud',
                'pytorch','computer vision', 'deep neural networks','neural networks','amazon web services',
                'natural language processing','extract,transform,load','mysql','structured query language'
                'critical thinking','communication','problem solving','teamwork','ethics','business acumen',
                'interpersonal skills','curiosity','storytelling','adaptability','team player','collaboration',
                'time management','leadership','domain knowledge','creativity','decision making',
                'verbal communication','written communication']

library_wd_tech = ['html','css','javascript','debugging','git','github','libraries','frameworks','publishing web site',
                'photoshop','bootstrap','jquery','search engine optimization','seo','basic graphic design','front end','back end',
                'visual studio code','visual studio','figma','zeplin','sketch','gatspy','strapi','paint','canva','aws','azure',
                'amazon web services','sql','mysql','nosql','node','node.js','js','json','api','google charts','d3','d3.js','react',
                'angular','ember','vue','python','c','c++','ruby','ruby on rails', 'github','php','net','.net','java','c#',
                'linux','go','gcp','troubleshooting','problem solving','ux','ui','ux/ui','cloud computing','netlify']

library_wd_soft = ['critical thinking','communication','problem solving','teamwork','ethics','business acumen',
                'interpersonal skills','curiosity','storytelling','adaptability','team player','collaboration',
                'time management','leadership','domain knowledge','creativity','decision making',
                'verbal communication','written communication']

library_wd_general = ['html','css','javascript','debugging','git','github','libraries','frameworks','publishing web site',
            'photoshop','bootstrap','jquery','search engine optimization','seo','basic graphic design','front end','back end',
            'visual studio code','visual studio','figma','zeplin','sketch','gatspy','strapi','paint','canva','aws','azure',
            'amazon web services','sql','mysql','nosql','node','node.js','js','json','api','google charts','d3','d3.js','react',
            'angular','ember','vue','python','c','c++','ruby','ruby on rails', 'github','php','net','.net','java','c#',
            'linux','go','gcp','troubleshooting','problem solving','ux','ui','ux/ui','cloud computing','netlify', 'critical thinking',
            'communication','problem solving','teamwork','ethics','business acumen','interpersonal skills','curiosity',
            'storytelling','adaptability','team player','collaboration','time management','leadership','domain knowledge',
            'creativity','decision making','verbal communication','written communication']