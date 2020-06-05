import sys
import pandas as pd
from sqlalchemy import create_engine

def load_data(messages_filepath, categories_filepath):
    """
    This function loads the message and categories files 
    merge them and return the new dataframe 
    Input: 2 csv files path
        messages_filepath: messages file path
        categories: the categories dataset filepath
    Output: Pandas dataframe of the merged csv files
    """
    messages = pd.read_csv(messages_filepath)
    categories = pd.read_csv(categories_filepath)
    df = pd.merge(messages,categories,on='id')
    return df


def clean_data(df):
    """
        Takes a Dataframe and perform cleaning operations such as
        expanding the multiple categories into seperate columns, 
        extract categories values, replace the previous categories with new columns
        and removing duplicates
        Input: DataFrame
        Output: cleaned dataframe
    """
    categories = df.categories.str.split(pat=';', n=-1, expand=True)
    row = categories.loc[:0]
    category_colnames = row.apply(lambda x: x[0][:-2]).tolist()
    categories.columns = category_colnames
    for column in categories:
        # set each value to be the last character of the string
        categories[column] = categories[column].str[-1] 
    
        # convert column from string to numeric
        categories[column] = categories[column].astype("int64")
    df.drop(labels=['categories'],axis=1,inplace=True)
    
    # concatenate the original dataframe with the new `categories` dataframe
    df = pd.concat([df,categories],axis=1)
    
    # drop duplicates
    df.drop_duplicates(inplace=True)
    return df


def save_data(df, database_filename):
    """
    Save the cleaned dataframe into the given database 
    input: 
        df: dataframe
        database_filename: database to store the cleaned dataframe 
    
    """
    engine = create_engine('sqlite:///'+ database_filename)
    df.to_sql('messages', engine, index=False)  


def main():
    if len(sys.argv) == 4:

        messages_filepath, categories_filepath, database_filepath = sys.argv[1:]

        print('Loading data...\n    MESSAGES: {}\n    CATEGORIES: {}'
              .format(messages_filepath, categories_filepath))
        df = load_data(messages_filepath, categories_filepath)

        print('Cleaning data...')
        df = clean_data(df)
        
        print('Saving data...\n    DATABASE: {}'.format(database_filepath))
        save_data(df, database_filepath)
        
        print('Cleaned data saved to database!')
    
    else:
        print('Please provide the filepaths of the messages and categories '\
              'datasets as the first and second argument respectively, as '\
              'well as the filepath of the database to save the cleaned data '\
              'to as the third argument. \n\nExample: python process_data.py '\
              'disaster_messages.csv disaster_categories.csv '\
              'DisasterResponse.db')


if __name__ == '__main__':
    main()
