# -------- IMPORT STATEMENTS --------
import pandas as pd
import os 
import requests
import json
from pandas import DataFrame
from dotenv import load_dotenv 
from azure.storage.blob import BlobServiceClient

load_dotenv()

# ------- ENV VARIABLES -------
# Retrieve connection string for Azure Storage in Azure portal use 
connect_str = os.getenv('AZURE_STORAGE_CONNECTION_STRING')
endpoint = os.getenv('endpoint')
subscription = os.getenv('subscription_key')

# ------- OTHER VARIABLES ------
headers = {"Ocp-Apim-Subscription-Key": subscription}
keyphrase_url = endpoint + "/text/analytics/v3.0/keyphrases" 
sentiment_url = endpoint + "/text/analytics/v3.0/sentiment" 

# -------- FUNCTIONS ----------

# Translates given string data into a JSON format; return appropriate request body needed to call API 
def text_to_json(textdata):
    documents = {"documents": [
    {"id": "1", "language": "en",
        "text": textdata}
    ]}

    return documents

# Call to Text Analytics API to get either the Key Phrases or Sentiment 
# Param: url, key value
def call_text_analytics(documents, url, keyvalue):
    # Use Requests library to send the request body to the API
    response = requests.post(url, headers=headers, json=documents)
    info = response.json()

    # Parse JSON response body to return the list of key phrases or sentiment 
    for d in documents:
        parseDict = info[d]
    for eachId in parseDict:
        resp = eachId[keyvalue]
    return resp


# ------- Code Block ---------
try:
    
    # Pull in data from blob 
    df = pd.read_csv(os.getenv('source'), dtype='unicode')

    # Parse out the text column from dataframe, dropping rows with NaN (empty rows)
    df = df.dropna()

    # ---------- KEY PHRASES ----------- #
    # Get Key Phrases from Text column of dataframe and insert them into a new column called Key Phrases 
    df["Key Phrases"] = df["Text"].apply(text_to_json).apply(call_text_analytics, args=(keyphrase_url, 'keyPhrases'))

    # ---------- SENTIMENT ----------- # 
    # Get Sentiment from Text column of dataframe and insert them into a new column called Sentiment  
    df["Sentiment"] = df["Text"].apply(text_to_json).apply(call_text_analytics, args=(sentiment_url, 'sentiment'))

    # Write back to blob storage (.csv file)
    df.to_csv("data-output.csv", index=False)
    
#Basic Exception Handling
except Exception as ex:
    print('Exception:')
    print(ex)