from langchain_huggingface import HuggingFaceEmbeddings
from langchain_qdrant import QdrantVectorStore
from dotenv import load_dotenv
import os
import logging

# Load environment variables from .env file
dotenv_path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), '.env')
load_dotenv(dotenv_path=dotenv_path)
logger = logging.getLogger(__name__)

class Retriever:
    _instance = None
    _initialized = False
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(Retriever, cls).__new__(cls)
        return cls._instance    
    
    def __init__(self):
        if not Retriever._initialized:
            try:
                # Validate environment variables
                embedding_model_name = os.getenv("EMBEDDING_MODEL")
                qdrant_url = os.getenv("QDRANT_URL")
                qdrant_api_key = os.getenv("QDRANT_API_KEY")
                qdrant_collection = os.getenv("QDRANT_COLLECTION")
                
                if not embedding_model_name:
                    raise ValueError("EMBEDDING_MODEL environment variable is not set")
                if not qdrant_url:
                    raise ValueError("QDRANT_URL environment variable is not set")
                if not qdrant_api_key:
                    raise ValueError("QDRANT_API_KEY environment variable is not set")
                if not qdrant_collection:
                    raise ValueError("QDRANT_COLLECTION environment variable is not set")
                
                logger.info(f"Khởi tạo embedding model: {embedding_model_name}")
                self.embedding_model = HuggingFaceEmbeddings(
                    model_name=embedding_model_name, 
                    model_kwargs={"trust_remote_code": True}
                )
                
                logger.info(f"Kết nối đến Qdrant: {qdrant_url}, collection: {qdrant_collection}")
                self.vector_store = QdrantVectorStore.from_existing_collection(
                    url=qdrant_url,
                    api_key=qdrant_api_key,
                    collection_name=qdrant_collection,
                    embedding=self.embedding_model
                )
                logger.info("Retriever khởi tạo thành công")
                Retriever._initialized = True
            except Exception as e:
                logger.error(f"Lỗi khi khởi tạo Retriever: {str(e)}")
                raise
    
    def retrieve(self, question, k = 20):
        if not Retriever._initialized:
            self.__init__()
        
        try:
            logger.debug(f"Đang embed câu hỏi: {question[:50]}...")
            question_embedding = self.embedding_model.embed_query(question)
            
            logger.debug(f"Đang tìm kiếm trong vector store với k={5}")
            result = self.vector_store.similarity_search_by_vector(
                question_embedding,
                k=5
            )
            logger.debug(f"Tìm thấy {len(result)} documents")
            return result
        except Exception as e:
            logger.error(f"Lỗi khi retrieve documents: {str(e)}")
            raise

    def get_content(self, question):
        try:
            docs = self.retrieve(question)
            contents = [doc.page_content.strip() for doc in docs]
            logger.debug(f"Đã lấy được {len(contents)} nội dung documents")
            return "\n".join(contents)
        except Exception as e:
            logger.error(f"Lỗi khi get_content: {str(e)}")
            raise
    
