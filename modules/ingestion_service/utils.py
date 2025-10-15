"""Utility functions for loading models and connecting to services."""
import pika
from sentence_transformers import SentenceTransformer
from pymilvus import connections, FieldSchema, CollectionSchema, DataType, Collection, utility
from minio import Minio

from . import config

def get_embedding_model() -> SentenceTransformer:
    """Loads and returns the sentence-transformer model."""
    print(f"Loading embedding model: '{config.EMBEDDING_MODEL}'...")
    model = SentenceTransformer(config.EMBEDDING_MODEL)
    print("Embedding model loaded.")
    return model

def get_minio_client() -> Minio:
    """Initializes and returns the MinIO client."""
    print(f"Initializing MinIO client for {config.MINIO_HOST}...")
    client = Minio(
        config.MINIO_HOST,
        access_key=config.MINIO_ACCESS_KEY,
        secret_key=config.MINIO_SECRET_KEY,
        secure=False
    )
    print("MinIO client initialized.")
    return client

def get_milvus_collection() -> Collection:
    """Connects to Milvus and returns the collection object, creating it if necessary."""
    print(f"Connecting to Milvus at {config.MILVUS_HOST}:{config.MILVUS_PORT}...")
    connections.connect("default", host=config.MILVUS_HOST, port=config.MILVUS_PORT)

    if not utility.has_collection(config.COLLECTION_NAME):
        print(f"Collection '{config.COLLECTION_NAME}' does not exist. Creating it...")
        fields = [
            FieldSchema(name="id", dtype=DataType.INT64, is_primary=True, auto_id=True),
            FieldSchema(name="embedding", dtype=DataType.FLOAT_VECTOR, dim=config.EMBEDDING_DIM),
            FieldSchema(name="text", dtype=DataType.VARCHAR, max_length=65535),
            FieldSchema(name="source", dtype=DataType.VARCHAR, max_length=1024)
        ]
        schema = CollectionSchema(fields, "Document chunks for RAG system")
        collection = Collection(config.COLLECTION_NAME, schema)

        index_params = {
            "metric_type": "L2",
            "index_type": "HNSW",
            "params": {"M": 8, "efConstruction": 64}
        }
        collection.create_index("embedding", index_params)
        print("Collection and index created.")
    else:
        print(f"Using existing collection: '{config.COLLECTION_NAME}'.")
        collection = Collection(config.COLLECTION_NAME)

    collection.load()
    print("Milvus collection loaded and ready.")
    return collection

def get_rabbitmq_connection() -> pika.BlockingConnection:
    """Establishes and returns a connection to RabbitMQ using credentials."""
    print(f"Connecting to RabbitMQ at {config.RABBITMQ_HOST}...")
    credentials = pika.PlainCredentials(config.RABBITMQ_USER, config.RABBITMQ_PASS)
    parameters = pika.ConnectionParameters(host=config.RABBITMQ_HOST, credentials=credentials)
    return pika.BlockingConnection(parameters)
