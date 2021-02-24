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
    if save_file == "Y":
        print("Enter the INITIALS of the job title:")
        initials = input()
        file_name = f"{initials}_top{k}_{library_type}_skills.json"
        df_skills.head(k).to_json(file_name, orient='records')
        s3 = boto3.resource('s3')
        s3.Bucket("additionaljobinfo").upload_file(file_name, file_name)
    elif save_file == "N":
        print("The dataframe has NOT been saved.")
    return df_skills.head(k)