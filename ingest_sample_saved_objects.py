import json
from copy import deepcopy
from datetime import datetime
from typing import List

from boto3 import resource
from faker import Faker
from faker.providers import company
from sys import argv
from uuid import uuid4

from constants import *


def insert_records(
        num_records: int = 10,
        saved_objects_file_path: str = 'sample_saved_objects.ndjson',
        replace_timestamp: bool = True,
        replace_title: bool = True
):
    """
    Writes/processes the provided saved objects json/ndjson file into DDB

    :param int num_records: Generates num_records of each saved objects in the saved_objects_file_path
    :param str saved_objects_file_path: Path to NDJSON/JSON file with saved objects (JSON can only contain 1 saved object)
    :param bool replace_timestamp: Replace timestamp with current datetime
    :param bool replace_title: Replace title with randomly generated title
    :return:
    """
    # Enforce types
    num_records = int(num_records)
    replace_timestamp = bool(replace_timestamp)
    replace_title = bool(replace_title)

    dynamodb = resource('dynamodb')
    table = dynamodb.Table(DDB_TABLE_NAME)

    with open(saved_objects_file_path) as file:
        sample_records: List[dict] = [json.loads(line) for line in file] \
            if is_ndjson_file(saved_objects_file_path) \
            else [json.load(file)]

    for _ in range(num_records):
        records = [
            process_saved_object(
                deepcopy(saved_object),
                len(sample_records * num_records) > 1,
                replace_timestamp,
                replace_title
            ) for saved_object in sample_records
        ]

        with table.batch_writer() as writer:
            for item in records:
                writer.put_item(Item=item)

    print("Finished inserting " + str(num_records * len(records)) + " records.")


def process_saved_object(
        saved_object: dict,
        append_id: bool,
        replace_timestamp: bool = True,
        replace_title: bool = True
) -> dict:
    """
    Adds random info to the following fields inside a saved object:
        - id (Append if num_records > 1)
        - attributes.title (Replace if specified)
        - attributes.references[i].id (Append if num_records > 1)
        - references[i].id (Append if num_records > 1)
        - updated_at (Replace if specified)

    :param dict saved_object: The object to process
    :param bool append_id: Whether to append a UUID to the existing id
    :param bool replace_timestamp: Whether to replace timestamp with current datetime
    :param bool replace_title: Whether to replace title with randomly generated title
    :return:
    """
    fake = Faker()
    fake.add_provider(company)

    if append_id and SAVED_OBJECTS_ID in saved_object:
        saved_object[SAVED_OBJECTS_ID] += str(uuid4())

    if SAVED_OBJECTS_ATTRIBUTES in saved_object:
        if replace_title and SAVED_OBJECTS_TITLE in saved_object[SAVED_OBJECTS_ATTRIBUTES]:
            saved_object[SAVED_OBJECTS_ATTRIBUTES][SAVED_OBJECTS_TITLE] = fake.bs()

        # object.attributes.references
        if append_id and SAVED_OBJECTS_REFERENCES in saved_object[SAVED_OBJECTS_ATTRIBUTES]:
            saved_object[SAVED_OBJECTS_ATTRIBUTES][SAVED_OBJECTS_REFERENCES] = [
                {**reference, SAVED_OBJECTS_ID: reference[SAVED_OBJECTS_ID] + str(uuid4())}
                if SAVED_OBJECTS_ID in reference
                else reference
                for reference in saved_object[SAVED_OBJECTS_ATTRIBUTES][SAVED_OBJECTS_REFERENCES]
            ]

    # object.references
    if append_id and SAVED_OBJECTS_REFERENCES in saved_object:
        saved_object[SAVED_OBJECTS_REFERENCES] = [
            {**reference, SAVED_OBJECTS_ID: reference[SAVED_OBJECTS_ID] + str(uuid4())}
            if SAVED_OBJECTS_ID in reference
            else reference
            for reference in saved_object[SAVED_OBJECTS_REFERENCES]
        ]

    if replace_timestamp and SAVED_OBJECTS_UPDATED_AT in saved_object:
        saved_object[SAVED_OBJECTS_UPDATED_AT] = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'

    return saved_object


def is_ndjson_file(filename: str) -> bool:
    return filename.endswith('.ndjson')


if __name__ == "__main__":
    if len(argv) > 5:
        print(
            "Usage: python ingest_sample_saved_objects.py "
            "[num_records] "
            "[saved_objects_file_path] "
            "[replace_timestamp] "
            "[replace_title]"
        )
    else:
        insert_records(*argv[1:])
