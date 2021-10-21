import datetime
import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore
from google.cloud import storage
import requests
import logging
import os
import pytz
import sys
import time
import traceback

logger = logging.getLogger("[LifecycleManager]")
logger.setLevel(logging.INFO)
formatter = logging.Formatter(
    '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler = logging.StreamHandler()
handler.setFormatter(formatter)
logger.addHandler(handler)

PROJECT_ID = os.environ.get("PROJECT_ID")
CREDENTIAL_PATH = os.environ.get("CREDENTIAL_PATH")
BUCKET_NAME = os.environ.get('BUCKET_NAME')
COLLECTION_NAME = os.environ.get('COLLECTION_NAME')

UNDELETABLE_LIFETIME = [u'forever', u'once']
DELETABLE_LIFETIME = [u'6h', u'12h', u'18h', u'24h']


class LifecycleManager():
    def __init__(self, db, bucket, collection_name):
        self.db = db
        self.bucket = bucket
        self.collection_name = collection_name

    def run_lifecycle_manager(self):
        self._check_slide_validity()
        self._mark_for_delete()
        self._delete_slides_and_documents()

    def _delete_blob(self, blob_name):
        blob = self.bucket.blob(blob_name)
        blob.delete()

        logger.info(f"Blob {blob_name} deleted")

    def _get_blob_size(self, blob_name):
        blob = self.bucket.get_blob(blob_name)
        return blob.size


    def _check_slide_validity(self):
        ''' implementing validating overwatcher '''
        now = datetime.datetime.now(tz=pytz.utc)
        before_one_hour_in_str = (now - datetime.timedelta(hours=1)).isoformat()

        new_docs = self.db.collection(u'{}'.format(self.collection_name)).where(u'created', u'>=', before_one_hour_in_str).where(u'isValid', u'==', 'false').stream()

        for doc in new_docs:
            document = doc.to_dict()
            try:
                file_size = self._get_blob_size(document['name'])
                if file_size == document['size']:
                    self.db.collection(u'{}'.format(self.collection_name)).document(u'{}'.format(doc.id)).update({u'isValid' : 'true'})
            except AttributeError as ae:
                continue


    def _mark_for_delete(self):
        ''' 
            Implementing delete process for overwatching documents

            Step.1
                find doucments with some conditions(lifetime, refCount, deleteFlag = false, isValid = true)
                set deleteFlag as true if it's liftetime is over
                set updated as current date

            Step.2
                gather documents which its deleteFlag is true
                compare current time and last updated time
                send delete call to storage if current time > updated time + 1h
        '''
            
        docs = self.db.collection(u'{}'.format(self.collection_name)).where(u'lifetime', u'in', DELETABLE_LIFETIME).where(u'deleteFlag', u'==', False).stream()
        for doc in docs:
            current_time = datetime.datetime.now(tz=pytz.utc)
            lifetime_in_doc = doc.to_dict()['lifetime']
            if lifetime_in_doc == '6h':
                time_pivot = (current_time - datetime.timedelta(hours=6)).isoformat()
            elif lifetime_in_doc == '12h':
                time_pivot = (current_time - datetime.timedelta(hours=12)).isoformat()
            elif lifetime_in_doc == '18h':
                time_pivot = (current_time - datetime.timedelta(hours=18)).isoformat()
            else:
                time_pivot = (current_time - datetime.timedelta(hours=24)).isoformat()

            if time_pivot > doc.to_dict()['created']:
                self.db.collection(u'{}'.format(self.collection_name)).document(u'{}'.format(doc.id)).update({u'deleteFlag' : True})


    def _delete_slides_and_documents(self):
        ''' TODO: implement removal overwatcher
            1. find documents which's deleteflag set as true
            2. delete the document's slide from cloud storage
            3. delete the document from firebase
        '''

        docs = self.db.collection(u'{}'.format(self.collection_name)).where(u'deleteFlag', u'==', True).where(u'refCount', u'==', 0).stream()
        for doc in docs:
            try:
                self._delete_blob(doc.to_dict()['name'])
            except Exception as e:
                logger.error(e)
                logger.error(traceback.format_exc(limit=None))
            self.db.collection(u'{}'.format(self.collection_name)).document(u'{}'.format(doc.id)).delete()



if __name__ == '__main__':
    cred = credentials.Certificate(f'{CREDENTIAL_PATH}')
    firebase_admin.initialize_app(cred, {
        'projectId': f"{PROJECT_ID}",
    })

    db = firestore.client()
    storage_client = storage.Client()
    bucket = storage_client.bucket(BUCKET_NAME)

    lcm = LifecycleManager(db, bucket, COLLECTION_NAME)
    while True:
        lcm.run_lifecycle_manager()
        time.sleep(1)


