# General Libraries
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# NLP Libraries
import nltk

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
    This function accepts a prepared dataframe with the job descriptioins, a positive integer k, and 
    a library of skills then returns a dataframe containing the top k skills needed .
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
    return df_skills.head(k)