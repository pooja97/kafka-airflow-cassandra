import logging
import time 
from confluent_kafka import Producer

logging.basicConfig(level = logging.INFO, format='%(asctime)s,[%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

class kafkaProducerWrapper:
    def __init__(self,bootstrap_servers):
        '''
        Initialize the kafka producer with the given bootstrap servers
        '''

        self.producer_config ={
            'bootstrap.servers': bootstrap_servers
        }
        
        self.producer = Producer(self.producer_config)

    def produce_message(self, topic, key,value):
        '''
        Produce a message to the specified kafka topic
        '''
        self.producer.produce(topic, key=key, value=value)
        self.producer.flush()

def kafka_producer_main():
    bootstrap_servers = 'kafka1:19092,kafka2:19093,kafka3:19094'
    kafka_producer = kafkaProducerWrapper(bootstrap_servers)

    topic = "email_topic"
    key = "sample_email@my_email.com"
    value = "1234567"

    start_time = time.time()

    try:
        while True:
            kafka_producer.produce_message(topic,key,value)
            logger.info("message produced")

            elapsed_time = time.time() - start_time
            if elapsed_time >=20:
                break 
            time.sleep(5)
    except KeyboardInterrupt:
        logger.info("Keyboard Interrupt Received. Stopping Producer.")

    finally:
        kafka_producer.producer.flush()
        logger.info("Producer flushed")

if __name__ == "__main__":
    kafka_producer_main()
    



