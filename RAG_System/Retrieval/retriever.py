from langchain_huggingface import HuggingFaceEmbeddings
from langchain_qdrant import QdrantVectorStore
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
        if not Retriever._initialized:
            
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
            
            Retriever._initialized = True
    
    def retrieve(self, question, k = 20):
        if not Retriever._initialized:
            self.__init__()
        
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
    
