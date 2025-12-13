from langchain_huggingface import HuggingFaceEmbeddings
from qdrant_client import QdrantClient
from langchain_qdrant import QdrantVectorStore
from qdrant_client.http.models import Distance, VectorParams, PointStruct
from tqdm.auto import tqdm
import pandas as pd
import uuid
import os
from dotenv import load_dotenv
from Data_Cleaning_and_Chunking import full_pipeline_Cleaning_and_Chunking

load_dotenv()

model = HuggingFaceEmbeddings(
    model_name="nomic-ai/nomic-embed-text-v1.5", 
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
    embedding = model 
)

chunked_documents = full_pipeline_Cleaning_and_Chunking("D:\RAG_for_Biodiversity_Assessment\DATA\Raw")
print(len(chunked_documents))
# for i in range(10):
#     print(chunked_documents[i])
#     print(chunked_documents[i].get("page_content"))
#     print()