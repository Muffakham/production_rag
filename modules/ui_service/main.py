"""Gradio UI for uploading documents to MinIO and triggering ingestion."""
import os
import gradio as gr
import pika
from minio import Minio

# --- Configuration ---
RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "localhost")
RABBITMQ_QUEUE = "document_queue"
RABBITMQ_USER = os.getenv("RABBITMQ_USER", "user")
RABBITMQ_PASS = os.getenv("RABBITMQ_PASS", "password")

MINIO_HOST = os.getenv("MINIO_HOST", "localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_BUCKET = "documents"

# --- Initialize MinIO Client ---
print("Initializing MinIO client...")
minio_client = Minio(
    MINIO_HOST,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=False # Set to True if using HTTPS
)
print("MinIO client initialized.")

def ingest_document(uploaded_file):
    """Uploads a file to MinIO and sends a message to RabbitMQ."""
    if uploaded_file is None:
        return "Please upload a file first."

    try:
        temp_path = uploaded_file.name
        object_name = os.path.basename(temp_path)

        # --- Ensure bucket exists ---
        found = minio_client.bucket_exists(MINIO_BUCKET)
        if not found:
            minio_client.make_bucket(MINIO_BUCKET)
            print(f"Bucket '{MINIO_BUCKET}' created.")
        else:
            print(f"Bucket '{MINIO_BUCKET}' already exists.")

        # --- Upload file to MinIO ---
        minio_client.fput_object(
            MINIO_BUCKET,
            object_name,
            temp_path,
        )
        print(f"File '{object_name}' uploaded to MinIO bucket '{MINIO_BUCKET}'.")

        # --- Send message to RabbitMQ ---
        credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=RABBITMQ_HOST, credentials=credentials)
        )
        channel = connection.channel()
        channel.queue_declare(queue=RABBITMQ_QUEUE, durable=True)

        message = object_name # The message is now the object name in the bucket

        channel.basic_publish(
            exchange='',
            routing_key=RABBITMQ_QUEUE,
            body=message,
            properties=pika.BasicProperties(delivery_mode=2)
        )
        connection.close()
        
        success_message = f"✅ Successfully sent '{object_name}' for ingestion."
        print(success_message)
        return success_message

    except Exception as e:
        error_message = f"❌ An error occurred: {e}"
        print(error_message)
        return error_message

# --- Create the Gradio Interface ---
with gr.Blocks() as demo:
    gr.Markdown("## Document Ingestion Service")
    gr.Markdown("Upload a text file to add it to the knowledge base.")
    
    file_input = gr.File(label="Upload Document")
    output_text = gr.Textbox(label="Status", interactive=False)
    
    ingest_button = gr.Button("Ingest Document")
    ingest_button.click(
        fn=ingest_document,
        inputs=file_input,
        outputs=output_text
    )

if __name__ == "__main__":
    # share=True is required to run inside a container
    demo.launch(server_name="0.0.0.0", server_port=7860, share=True)
