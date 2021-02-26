# General Libraries
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# NLP Libraries
import re
import nltk

# AWS Librareis
import boto3

############################################ Helper Functions ####################################################
def words_variables(df):
    '''
    This function accepts the prepared dataframe with the job descriptioins and return a dictionary 
    in which the values are the words that appear in the job description. 
    '''
    # Create the words that appear all the job descritipons
    all_words = ' '.join(df.clean)
    # Create a dictionary to hold the variable all_words
    d_words = {'frequency': all_words}
    return d_words

def everygram_frequency(d_words, max_len=3):
    '''
    This function accetps the dictionary produced by the function `words_variables` and 
    return mono-, bi-, and tri-grams along with their frequencies. 
    '''
    # Generate mono-, bi-, and tri-grams
    grams = nltk.everygrams(d_words['frequency'].split(), max_len=max_len) # dtype of grams: <class 'genertor'>
    # Convert to a list of tuples
    grams = list(grams)
    # Create an empty list to hold mono-, bi-, and tri-grams
    everygram = []
    # For loop the list of tuples and convert the tuple grams to string grams
    for gram in grams:
        str_gram = gram[0]
        for i in gram[1:]:
            str_gram = str_gram + ' ' + i
        everygram.append(str_gram)
    # Compute the frequency of the everygrams
    everygram = pd.Series(everygram).value_counts()
    return everygram

def top_skills(df, k, library, library_type):
    '''
    This function accepts a prepared dataframe with the job descriptioins, a positive integer k, a library of skills, 
    and the type of library then returns a dataframe containing the top k skills needed. In addition, it provides
    the option to save the datafrme as JSON file and upload to the AWS Bucket additionaljobinfo.
    '''
    # Create a string of all words that appear in the job description
    dic = words_variables(df)
    # Compute the words frequency
    gram_frequency = everygram_frequency(dic)
    # Create an empty dataframe to hold the rank of the skills
    df_skills = pd.DataFrame()
    # For loop through the library to find out the frequency of the skills mentioned in the job description
    for skill in library:
        mask = (gram_frequency.index == skill)
        df = gram_frequency[mask]
        df_skills = pd.concat([df_skills, df])
    df_skills.columns = dic.keys()
    df_skills.sort_values(by='frequency', ascending=False, inplace=True)
    # Reset the index
    df_skills.reset_index(inplace=True)
    # Rename the column name
    df_skills.rename(columns={'index': f'top{k}_{library_type}_skills'}, inplace=True)
    # Provide the option to save the dataframe as the JSON
    print("Do you want to save the dataframe as JSON and upload to AWS? (Y/N)")
    save_file = input()
    if save_file == "Y" or save_file == "y":
        print("Enter the INITIALS of the job title:")
        initials = input()
        file_name = f"{initials}_top{k}_{library_type}_skills.json"
        df_skills.head(k).to_json(file_name, orient='records')
        s3 = boto3.resource('s3')
        s3.Bucket("additionaljobinfo").upload_file(file_name, file_name)
    elif save_file == "N" or save_file == 'n':
        print("The dataframe has NOT been saved.")
    return df_skills.head(k)

def add_skill_frequency(df, df_top):
    '''
    This function accepts the dataframe of prepared job postings and the dataframe of the top k skills
    and adds the frequencies of the skills in each observation.
    '''
    # Reset the index of the df
    df_copy = df.reset_index()
    # Create a list of the top skills
    skill_list = df_top.iloc[:, 0].to_list()
    # Create an empty dictionary to hold the frequency of the skill in each observation
    dic_frequency = {}
    # Loop through the list of skills to compute its frequency in each observation
    for skill in skill_list:
        list_frequency = []
        for string in df_copy.clean.values:
            matches = re.findall(f" {skill} ", string)
            frequency = len(matches)
            list_frequency.append(frequency)
        dic_frequency[skill]=list_frequency    
    # Convert the dictionary into the dataframe
    df_frequency = pd.DataFrame(dic_frequency)
    # Add the frequencies of the top skills to the original dataframe
    df_copy = pd.concat([df_copy, df_frequency], axis=1)
    # Reset the date as the index
    df_copy = df_copy.set_index('date')
    return df_copy

def plot_top_skill_ts(df, df_top):
    '''
    This function accetps the dataframe of preapred job postings with the frequencies of the skills
    and plot how popular each skill changes over time. 
    '''
    # Set up the size of the plot
    plt.figure(figsize=(12, 8))
    # Create a list of the top skills
    skill_list = df_top.iloc[:, 0].to_list()
    # Resample the dataset by week and plot the mean of the frequency of each skill per job posting
    for skill in skill_list:
        df.resample('W')[skill].mean().plot(label=f'{skill} Weekly')
    
    # Name the plot
    plt.title("How Popular the Top 5 Skills Are Over Time", fontweight='bold')
    # Position the legend
    plt.legend(bbox_to_anchor=(1, 1))