import csv
from io import StringIO

from google.cloud import storage

storage_client = storage.Client()


def read_file(event, context):
    """Triggered by a change to a Cloud Storage bucket.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """
    file = event
    print(f"Processing file: {file['name']}.")
    bucket = storage_client.get_bucket(event['bucket'])
    blob = bucket.blob(event['name'])
    blob = blob.download_as_string()
    blob = blob.decode('utf-8')
    blob = StringIO(blob)  # tranform bytes to string here
    lines = csv.reader(blob)  # then use csv library to read the content
    listoflists = []
    for line in lines:
        rows = []
        line = line[0]
        RGN_NO = line[0:7:1]
        DSTR_NO = line[7:14:1]
        DSTR_DS = line[14:20:1]
        print(f"RGN_NO: {RGN_NO}")
        print(f"DSTR_NO: {DSTR_NO}")
        print(f"DSTR_DS: {DSTR_DS}")
        rows.append(RGN_NO)
        rows.append(DSTR_NO)
        rows.append(DSTR_DS)
        listoflists.append(rows)
    bucket = storage_client.get_bucket("gs://sprs_district_formatted")
    blob = bucket.blob("sprs_district_formatted.csv")
    blob.upload_from_string(
        data=listoflists,
        content_type='text/csv')
