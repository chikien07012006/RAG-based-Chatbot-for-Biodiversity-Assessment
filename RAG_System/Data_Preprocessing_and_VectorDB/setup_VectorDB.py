from langchain_huggingface import HuggingFaceEmbeddings
from qdrant_client import QdrantClient
from langchain_qdrant import QdrantVectorStore
from qdrant_client.http.models import Distance, VectorParams, PointStruct
from tqdm.auto import tqdm
import pandas as pd
from  uuid import  uuid4
import os
from dotenv import load_dotenv
from Data_Cleaning_and_Chunking import full_pipeline_Cleaning_and_Chunking

# Load environment variables from .env file
dotenv_path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), '.env')
load_dotenv(dotenv_path=dotenv_path)

model = HuggingFaceEmbeddings(
    model_name=os.getenv("EMBEDDING_MODEL"), 
    model_kwargs={"trust_remote_code": True})

# qdrant_client = QdrantClient(
#     url = os.getenv("QDRANT_URL"),
#     api_key = os.getenv("QDRANT_API_KEY")
# )

# qdrant_client.create_collection(
#     collection_name = os.getenv("QDRANT_COLLECTION"),
#     vectors_config = VectorParams(size=768, distance=Distance.COSINE),
# )

vector_store = QdrantVectorStore.from_existing_collection(
    url = os.getenv("QDRANT_URL"),
    api_key = os.getenv("QDRANT_API_KEY"),
    collection_name = os.getenv("QDRANT_COLLECTION"),
    embedding = model,
    timeout=300
)
chunked_documents = full_pipeline_Cleaning_and_Chunking(r"D:\RAG_for_Biodiversity_Assessment\DATA\Raw")

uuids = [str(uuid4()) for _ in range(len(chunked_documents))]

vector_store.add_documents(documents = chunked_documents, ids=uuids, batch_size=64)

print("done")
