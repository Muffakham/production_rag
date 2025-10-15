"""Contains the DocumentProcessor class for processing individual documents from MinIO."""
from langchain.text_splitter import RecursiveCharacterTextSplitter
from sentence_transformers import SentenceTransformer
from pymilvus import Collection
from minio import Minio
from . import config

class DocumentProcessor:
    """Encapsulates the logic to process a single document from an object store."""

    def __init__(self, collection: Collection, model: SentenceTransformer, minio_client: Minio):
        """Initializes the processor with clients for Milvus, MinIO, and an embedding model."""
        self.collection = collection
        self.model = model
        self.minio_client = minio_client
        self.text_splitter = RecursiveCharacterTextSplitter(
            chunk_size=config.CHUNK_SIZE,
            chunk_overlap=config.CHUNK_OVERLAP
        )

    def process(self, object_name: str) -> None:
        """Downloads, chunks, embeds, and indexes a single document from MinIO."""
        print(f"Processing document from MinIO: '{object_name}'...")
        try:
            # Download the document from MinIO
            response = self.minio_client.get_object(config.MINIO_BUCKET, object_name)
            text = response.read().decode('utf-8')
            response.close()
            response.release_conn()

            if not text.strip():
                print(f"Document {object_name} is empty. Skipping.")
                return

            chunks = self.text_splitter.split_text(text)
            print(f"Document split into {len(chunks)} chunks.")

            embeddings = self.model.encode(chunks, show_progress_bar=False)

            # The 'source' metadata will now be the object name
            source_metadata = [object_name] * len(chunks)

            entities = [
                embeddings,
                chunks,
                source_metadata
            ]

            self.collection.insert(entities)
            self.collection.flush()
            print(f"Successfully inserted {len(chunks)} chunks from {object_name} into Milvus.")

        except Exception as e:
            print(f"An unexpected error occurred while processing {object_name}: {e}")
