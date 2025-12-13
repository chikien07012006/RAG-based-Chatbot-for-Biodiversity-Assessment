from langchain_huggingface import HuggingFaceEmbeddings
from langchain_qdrant import QdrantVectorStore
from langchain_community.cross_encoders import HuggingFaceCrossEncoder
from langchain.retrievers.document_compressors import CrossEncoderReranker
from dotenv import load_dotenv
import os

load_dotenv()

class Retriever:
    _instance = None
    _initialized = False
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(Retriever, cls).__new__(cls)
        return cls._instance
    
    def __init__(self):
        if not self._initialized:
            
            self.embedding_model = HuggingFaceEmbeddings(
            model_name=os.getenv("EMBEDDING_MODEL"), 
            model_kwargs={"trust_remote_code": True}
            )
            
            self.vector_store = QdrantVectorStore.from_existing_collection(
            url = os.getenv("QDRANT_URL"),
            api_key = os.getenv("QDRANT_API_KEY"),
            collection_name = os.getenv("QDRANT_COLLECTION"),
            embedding = self.embedding_model
            )
            
            self._initialized = True
    
    def retrieve(self, question, k = 20):
        question_embedding = self.embedding_model.embed_query(question)
    
        result = self.vector_store.similarity_search_by_vector(
            question_embedding,
            k = 5
        )
    
        return result

    def get_content(self, question):
        docs = self.retrieve(question)
        contents = [doc.page_content.strip() for doc in docs]
        
        return "\n".join(contents)
    
Retriever1 = Retriever()
print(Retriever1.get_content("San hô là động vật hay thực vật? Chúng sống bằng cách nào?"))


# # # startup.py hoặc main.py
# from fastapi import FastAPI

# app = FastAPI()

# @app.on_event("startup")
# async def startup_event():
#     load_dotenv()
#     app.state.embedding_model = HuggingFaceEmbeddings(
#         model_name="nomic-ai/nomic-embed-text-v1.5",
#         model_kwargs={"trust_remote_code": True}
#     )
#     app.state.vector_store = QdrantVectorStore.from_existing_collection(
#         url=os.getenv("QDRANT_URL"),
#         api_key=os.getenv("QDRANT_API_KEY"),
#         collection_name=os.getenv("QDRANT_COLLECTION"),
#         embedding=app.state.embedding_model
#     )

# @app.post("/retrieve")
# async def retrieve(question: str):
#     question_embedding = app.state.embedding_model.embed_query(question)
#     results = app.state.vector_store.similarity_search_by_vector(
#         question_embedding, k=20, fetch_k=60
#     )
#     return results