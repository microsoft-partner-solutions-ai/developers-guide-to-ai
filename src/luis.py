# -------- IMPORT STATEMENTS --------
from azure.cognitiveservices.language.luis.authoring import LUISAuthoringClient
from azure.cognitiveservices.language.luis.runtime import LUISRuntimeClient
from msrest.authentication import CognitiveServicesCredentials
import pandas as pd
import numpy as np
import json, time
import os
from dotenv import load_dotenv

load_dotenv()

# ------- ENV VARIABLES -------
authoringKey = os.getenv('AUTHORINGKEY')
authoringResourceName = os.getenv('AUTHORINGRESOURCENAME')
predictionResourceName = os.getenv('PREDICTIONRESOURCENAME')

# ------- OTHER VARIABLES ------
authoringEndpoint = 'https://' + authoringResourceName +  '.cognitiveservices.azure.com/'
predictionEndpoint = 'https://' + predictionResourceName + '.cognitiveservices.azure.com/'
appName = "Tailwind Traders Test"
versionId = "0.1"


# Pull up data into data frame and drop rows with NaN (empty rows)
data = pd.read_csv("./data/data-output.csv")
data = data.dropna()

intents = []
for index, row in data.iterrows():        
    intents.append({"intentName":row["Theme"], 'text':row["Text"]})

print(intents)

# Authenticate the client with Cognitive Service Credentials
client = LUISAuthoringClient(authoringEndpoint, CognitiveServicesCredentials(authoringKey))

# Define app basics
appDefinition = {
    "name": appName,
    "initial_version_id": versionId,
    "culture": "en-us"
}

# Create app
app_id = client.apps.add(appDefinition)

# Create intents 
client.model.add_intent(app_id, versionId, "speed")
client.model.add_intent(app_id, versionId, "features")
client.model.add_intent(app_id, versionId, "other")
client.model.add_intent(app_id, versionId, "support")
client.model.add_intent(app_id, versionId, "services")
client.model.add_intent(app_id, versionId, "price")
client.model.add_intent(app_id, versionId, "design")
client.model.add_intent(app_id, versionId, "reliability")
client.model.add_intent(app_id, versionId, "security")


for i in intents:
    client.examples.add(app_id, versionId, i)

# Train the app
client.train.train_version(app_id, versionId)
waiting = True
while waiting:
    info = client.train.get_status(app_id, versionId)

    # get_status returns a list of training statuses, one for each model. Loop through them and make sure all are done.
    waiting = any(map(lambda x: 'Queued' == x.details.status or 'InProgress' == x.details.status, info))
    if waiting:
        print ("Waiting 10 seconds for training to complete...")
        time.sleep(10)
    else: 
        print ("trained")
        waiting = False

# Publish a Language Understanding app
responseEndpointInfo = client.apps.publish(app_id, versionId, is_staging=False)

# Authenticate the prediction runtime client
runtimeCredentials = CognitiveServicesCredentials(authoringKey)
clientRuntime = LUISRuntimeClient(endpoint=predictionEndpoint, credentials=runtimeCredentials)

