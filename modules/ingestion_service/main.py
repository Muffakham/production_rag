"""Main entry point for the document ingestion service."""
import pika
import time

from . import config
from . import utils
from .document_processor import DocumentProcessor

def main():
    """Initializes components and starts the RabbitMQ consumer loop."""
    # 1. Load the embedding model
    embedding_model = utils.get_embedding_model()

    # 2. Initialize the Milvus collection
    milvus_collection = utils.get_milvus_collection()

    # 3. Initialize the MinIO client
    minio_client = utils.get_minio_client()

    # 4. Initialize the document processor with all clients
    processor = DocumentProcessor(milvus_collection, embedding_model, minio_client)

    # 5. Set up and run the RabbitMQ consumer
    while True:
        connection = None
        try:
            connection = utils.get_rabbitmq_connection()
            channel = connection.channel()
            channel.queue_declare(queue=config.RABBITMQ_QUEUE, durable=True)
            print("RabbitMQ connection successful.")

            def callback(ch, method, properties, body):
                object_name = body.decode()
                print(f" [x] Received message to process object: {object_name}")
                
                # Use the processor to handle the document from MinIO
                processor.process(object_name)
                
                ch.basic_ack(delivery_tag=method.delivery_tag)

            channel.basic_qos(prefetch_count=1)
            channel.basic_consume(queue=config.RABBITMQ_QUEUE, on_message_callback=callback)

            print("[*] Waiting for messages. To exit press CTRL+C")
            channel.start_consuming()

        except pika.exceptions.AMQPConnectionError as e:
            print(f"Connection to RabbitMQ failed: {e}. Retrying in 5 seconds...")
            time.sleep(5)
        except KeyboardInterrupt:
            print("Consumer stopped by user.")
            break
        except Exception as e:
            print(f"An unexpected error occurred: {e}. Shutting down.")
            break
        finally:
            if connection and not connection.is_closed:
                connection.close()
                print("RabbitMQ connection closed.")

if __name__ == "__main__":
    main()
