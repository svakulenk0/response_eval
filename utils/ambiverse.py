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
ALTERNATIVE_REPLY_COLUMN = 'User_Reply'

# new columns with semantic annotations
CONTEXT_MATCHES_COLUMN = 'Dialogue_Context_Matches'
REPLY_MATCHES_COLUMN = 'Reply_Matches'
CONTEXT_ENTITIES_COLUMN = 'Dialogue_Context_Entities'
REPLY_ENTITIES_COLUMN = 'Reply_Entities'
USER_REPLIES_MATCHES_COLUMN = 'User_Replies_Matches'
USER_REPLIES_ENTITIES_COLUMN = 'User_Replies_Entities'

# 1. load all dialogues
def load_dataset(path=DATASET_PATH):
    data = pd.read_csv(path, sep="\t")
    df = data[["Dialogue_ID", "Dialogue_Context", "Reply", "Label", "Age", "Gender", "English_Proficency", "User_Relevance_Score", "User_Reply", "Task_ID"]]
    # return df.iloc[:10,:]
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
    if input_text:
        input_text = re.sub(r'%s'%ignore, '', input_text)
        data['text'] = input_text
        response = requests.post(ambiverse_api_url, headers=headers, data=json.dumps(data))
        return json.loads(response.text)
    else:
        return {}


ambiversed_dataset = []

print("Annotating...")
cursor = None
for index, x in dataset.iterrows():
    # next dialogue sample
    if cursor != x["Dialogue_ID"]:
        if cursor:
            ambiversed_dataset.append({"Dialogue_ID": cursor, "Label": x["Label"],
                                       REPLY_COLUMN: x[REPLY_COLUMN], CONTEXT_COLUMN: x[CONTEXT_COLUMN],
                                       REPLY_MATCHES_COLUMN: correct_reply_matches, CONTEXT_MATCHES_COLUMN: context_matches,
                                       REPLY_ENTITIES_COLUMN: correct_reply_entities, CONTEXT_ENTITIES_COLUMN: context_entities,
                                       "User_Relevance_Scores": scores, "User_Replies": alternative_replies,
                                       USER_REPLIES_MATCHES_COLUMN: reply_matches, USER_REPLIES_ENTITIES_COLUMN: reply_entities})
        # reset cursor
        cursor = x["Dialogue_ID"]
        scores, alternative_replies, reply_matches, reply_entities = [], [], [], []

        # parse context only once per dialogue sample
        context_annotations = ambiverse_annotation_request(x[CONTEXT_COLUMN])
        context_matches = context_annotations['matches']
        if 'entities' in context_annotations:
            context_entities = context_annotations['entities']
        else:
            context_entities = []

        # parse correct reply
        correct_reply_annotations = ambiverse_annotation_request(x[REPLY_COLUMN])
        if 'matches' in correct_reply_annotations:
            correct_reply_matches = correct_reply_annotations['matches']
        else:
            correct_reply_matches = []
        if 'entities' in correct_reply_annotations:
            correct_reply_entities = correct_reply_annotations['entities']
        else:
            correct_reply_entities = []
        label = x["Label"]

    print(cursor)

    # parse correct reply
    reply_annotations = ambiverse_annotation_request(x[ALTERNATIVE_REPLY_COLUMN])
    if 'entities' in reply_annotations:
        reply_matches.append(reply_annotations['matches'])
    else:
        reply_matches.append([])
    
    if 'entities' in reply_annotations:
        reply_entities.append(reply_annotations['entities'])
    else:
        reply_entities.append([])

    alternative_replies.append(x[ALTERNATIVE_REPLY_COLUMN])
    scores.append(x["User_Relevance_Score"])

# add last dialogue
ambiversed_dataset.append({"Dialogue_ID": cursor, "Label": x["Label"],
                           REPLY_COLUMN: x[REPLY_COLUMN], CONTEXT_COLUMN: x[CONTEXT_COLUMN],
                           REPLY_MATCHES_COLUMN: correct_reply_matches, CONTEXT_MATCHES_COLUMN: context_matches,
                           REPLY_ENTITIES_COLUMN: correct_reply_entities, CONTEXT_ENTITIES_COLUMN: context_entities,
                           "User_Relevance_Scores": scores, "User_Replies": alternative_replies,
                           USER_REPLIES_MATCHES_COLUMN: reply_matches, USER_REPLIES_ENTITIES_COLUMN: reply_entities})


with open(NEW_DATASET_PATH, 'w') as outfile:
    json.dump(ambiversed_dataset, outfile, indent=2)

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
print("Entities detected for %d out of %d samples"% (counter, len(dataset)))
