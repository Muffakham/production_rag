"""Configuration settings for the Ingestion Service."""
import os

# --- RabbitMQ Configuration ---
RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "localhost")
RABBITMQ_QUEUE = "document_queue"
RABBITMQ_USER = os.getenv("RABBITMQ_USER", "user")
RABBITMQ_PASS = os.getenv("RABBITMQ_PASS", "password")

# --- MinIO Configuration ---
MINIO_HOST = os.getenv("MINIO_HOST", "localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_BUCKET = "documents"

# --- Milvus Configuration ---
MILVUS_HOST = os.getenv("MILVUS_HOST", "localhost")
MILVUS_PORT = os.getenv("MILVUS_PORT", "19530")
COLLECTION_NAME = "document_chunks"

# --- Embedding Model Configuration ---
EMBEDDING_MODEL = 'all-MiniLM-L6-v2'
EMBEDDING_DIM = 384  # Based on the model 'all-MiniLM-L6-v2'

# --- Text Chunking Configuration ---
CHUNK_SIZE = 1000
CHUNK_OVERLAP = 150
