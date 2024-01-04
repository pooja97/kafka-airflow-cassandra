from confluent_kafka import Consumer
from pymongo import MongoClient
import logging
import time

logging.basicConfig(level = logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

class MongoDBConnector:
    def __init__(self, mongodb_uri, database_name, collection_name):
        self.client = MongoClient(mongodb_uri)
        self.db = self.client[database_name]
        self.collection_name = collection_name

    def create_collection(self):
        #check if collection already exists 
        if self.collection_name not in self.db.list_collection_names():
            self.db.create_collection(self.collection_name)
            logger.info(f"Create collection:{self.collection_name}")
        else:
            logger.warning(f"collection {self.collection_name} already exists")


    def insert_data(self, email, otp):
        document = {
            "email" : email,
            "otp" : otp 
        }
        self.db[self.collection_name].insert_one(document)

    def close(self):
        self.client.close() 

class KafkaConsumerWrapperMongoDB:
    def __init__(self, kafka_config, topics):
        self.consumer = Consumer(kafka_config)
        self.consumer.subscribe(topics)
    
    def consume_and_insert_messages(self):
        start_time = time.time()
        try:
            while True:
                elapsed_time = time.time() - start_time
                if elapsed_time >= 30:
                    break 
                msg = self.consumer.poll(1.0)
                if msg is None:
                    continue 

        except: 
            


    
