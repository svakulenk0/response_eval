#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''
Created on Mar 26, 2018

.. codeauthor: svitlana vakulenko
    <svitlana.vakulenko@gmail.com>

Annotate HUMOD dialogues with Ambiverse semantic annotation service
'''

import pandas as pd

DATASET_PATH = '../datas/HUMOD.TXT'
NEW_DATASET_PATH = '../datas/HUMOD_Ambiversed.TXT'

# existing columns
CONTEXT_COLUMN = 'Dialogue_Context'
REPLY_COLUMN = 'Reply'

# new columns with semantic annotations
CONTEXT_ENTITIES_COLUMN = 'Dialogue_Context_Entities'
REPLY_ENTITIES_COLUMN = 'Reply_Entities'

# 1. load all dialogues
def load_dataset(path=DATASET_PATH):
    data = pd.read_csv(path, sep="\t")
    df = data[["Dialogue_ID", "Dialogue_Context", "Reply", "Label"]]
    return df


dataset = load_dataset()
print(dataset.columns)

# produce semantic annotations for dialogue context and candidate reply text and save as JSON
import requests, json
import re

ambiverse_api_url = "http://localhost:9000/factextraction/analyze"
headers = {'content-type': 'application/json', 'accept': 'application/json'}
data = {"extractConcepts": "true", "language": "en"}


def ambiverse_annotation_request(input_text, ignore='Speaker'):
    input_text = re.sub(r'%s'%ignore, '', input_text)
    data['text'] = input_text
    response = requests.post(ambiverse_api_url, headers=headers, data=json.dumps(data))
    return json.loads(response.text)['matches']


ambiversed_dataset = []

print("Annotating...")
for index, x in dataset.iterrows():
    print(x["Dialogue_ID"])
    context_entities = ambiverse_annotation_request(x[CONTEXT_COLUMN])
    reply_entities = ambiverse_annotation_request(x[REPLY_COLUMN])
    ambiversed_dataset.append({"Dialogue_ID": x["Dialogue_ID"], "Label": x["Label"],
                               REPLY_COLUMN: x[REPLY_COLUMN], CONTEXT_COLUMN: x[CONTEXT_COLUMN],
                               REPLY_ENTITIES_COLUMN: reply_entities, CONTEXT_ENTITIES_COLUMN: context_entities,})

with open(NEW_DATASET_PATH, 'w') as outfile:
    json.dump(ambiversed_dataset, outfile)

# test read new annotated dataset
def load_annotated_dataset(path=NEW_DATASET_PATH):
    with open(path, "r") as read_file:
        dataset = json.load(read_file)
    return dataset

dataset = load_annotated_dataset()
counter = 0
for x in dataset:
    # show sample only if some entities were detected
    if x['Dialogue_Context_Entities'] or x['Reply_Entities']:
        counter += 1
        print(x['Dialogue_Context_Entities'])
        print(x['Reply_Entities'])
print("Entities detected for %d out of %d samples"% (counter, len(dataset))
