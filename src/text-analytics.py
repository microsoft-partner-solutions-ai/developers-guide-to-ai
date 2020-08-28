# -------- IMPORT STATEMENTS --------
import pandas as pd
import os, uuid 
import requests
import json
from pandas import DataFrame
from dotenv import load_dotenv 
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
from azure.storage.blob import BlobServiceClient, ResourceTypes, AccountSasPermissions, generate_account_sas
from datetime import datetime, timedelta
load_dotenv()

# -------- FUNCTIONS ----------

# Translates given string data into a JSON format; return appropriate request body needed to call API 
def col_to_json(textdata):
    documents = {"documents": [
    {"id": "1", "language": "en",
        "text": textdata}
    ]}
    return documents

# Grabs Key Phrases from Text Analytics API call given the appropriate request body 
def callKeyPhraseAndParse(documents):
    # Use Requests library to send the request body to the API
    headers = {"Ocp-Apim-Subscription-Key": os.getenv('subscription_key')}
    response = requests.post(keyphrase_url, headers=headers, json=documents)
    key_phrases = response.json()

    # Parse JSON response bodyto return the list of key phrases only 
    for d in documents:
        parseDict = key_phrases[d]
    for eachId in parseDict:
        keyPhraseValues = eachId['keyPhrases']
    return keyPhraseValues

# Grabs Sentiment from Text Analaytics API call given the appropriate request body
def callSentimentAndParse(documents):
    # Use Requests library to send the request body to the API
    headers = {"Ocp-Apim-Subscription-Key": os.getenv('subscription_key')}
    response = requests.post(sentiment_url, headers=headers, json=documents)
    sentiments = response.json()

    # Parse JSON response body to return just the sentiments 
    for d in documents:
        parseDict = sentiments[d]
    for eachId in parseDict:
        sentimentValues = eachId['sentiment']
    return sentimentValues

# ------- Code Block ---------
try:
    # Retrieve connection string for Azure Storage in Azure portal use 
    connect_str = os.getenv('AZURE_STORAGE_CONNECTION_STRING')

    # Create the BlobServiceClient object which will be used to create a container client
    blob_service_client = BlobServiceClient.from_connection_string(connect_str)
    
    # Pull in data from blob 
    source = 'https://developersguidetrial.blob.core.windows.net/devguideblob/modifieddata.csv?sp=r&st=2020-08-25T23:36:01Z&se=2020-10-30T07:36:01Z&spr=https&sv=2019-12-12&sr=b&sig=JuOyO8VelYEWAX0Ot7rZG6ZPAMAzWbBVHF%2FJtbRjJ5E%3D'
    df = pd.read_csv(source, dtype='unicode')

    # Parse out the text column from dataframe, dropping rows with NaN (empty rows)
    df = df.dropna()

    # Working with a subset of the data 
    df2 = df.head(10)
    #print(df2)

    # ---------- KEY PHRASES ----------- #
    # Get Key Phrases from Text column of dataframe and insert them into a new column called Key Phrases 
    keyphrase_url = os.getenv('endpoint') + "/text/analytics/v3.0/keyphrases" 
    df2["Key Phrases"] = df2["Text"].apply(col_to_json).apply(callKeyPhraseAndParse)
    # print(df2)

    # ---------- SENTIMENT ----------- # 
    # Get Sentiment from Text column of dataframe and insert them into a new column called Sentiment  
    sentiment_url = os.getenv('endpoint') + "/text/analytics/v3.0/sentiment" 
    df2["Sentiment"] = df2["Text"].apply(col_to_json).apply(callSentimentAndParse)
    print(df2)

    # Write back to blob storage (.csv file)
    df2.to_csv(r'C:\Users\dthakar\Documents\dev-guide\devguideblob\updateddata.csv', index=False)

#Basic Exception Handling
except Exception as ex:
    print('Exception:')
    print(ex)