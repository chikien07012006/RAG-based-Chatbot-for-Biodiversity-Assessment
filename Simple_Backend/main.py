import sys
from pathlib import Path
import traceback

# Add parent directory to Python path to import RAG_System
parent_dir = Path(__file__).parent.parent
if str(parent_dir) not in sys.path:
    sys.path.insert(0, str(parent_dir))

from fastapi import FastAPI, Depends, HTTPException
from contextlib import asynccontextmanager
from pydantic import BaseModel
from RAG_System import ResponseGenerator
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

logger = logging.getLogger(__name__)


class ChatRequest(BaseModel):
    question: str

# Singleton nhưng LAZY
_generator = None

def get_generator():
    global _generator
    if _generator is None:
        try:
            logger.info("Khởi tạo ResponseGenerator (lazy)...")
            _generator = ResponseGenerator()
            logger.info("ResponseGenerator khởi tạo thành công")
        except Exception as e:
            logger.error(f"Lỗi khi khởi tạo ResponseGenerator: {str(e)}")
            logger.error(traceback.format_exc())
            raise HTTPException(
                status_code=500, 
                detail=f"Không thể khởi tạo ResponseGenerator: {str(e)}"
            )
    return _generator

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifespan context manager to handle startup and shutdown events"""
    logger.info("Đang khởi động ứng dụng và load models (Embedding + LLM)...")
    try:
        # Force initialization of the generator
        generator = get_generator()
        generator.initialize()
        logger.info("Models đã được load thành công! Ứng dụng sẵn sàng xử lý request.")
    except Exception as e:
        logger.error(f"Lỗi nghiêm trọng khi load models: {e}")
        # Optional: raise e to prevent app from starting if models fail
    yield
    logger.info("Ứng dụng đang tắt...")

app = FastAPI(lifespan=lifespan)

@app.get("/")
def root():
    return {"status": "ok"}

@app.get("/ready")
def ready():
    """Health check endpoint"""
    return {"ready": True}

@app.get("/health")
def health():
    """Detailed health check with configuration status"""
    import os
    from pathlib import Path
    
    env_file = Path(__file__).parent.parent / ".env"
    env_exists = env_file.exists()
    
    # Check required environment variables (without exposing values)
    required_vars = [
        "EMBEDDING_MODEL",
        "QDRANT_URL",
        "QDRANT_API_KEY",
        "QDRANT_COLLECTION",
        "HUGGINGFACEHUB_API_TOKEN"
    ]
    
    env_status = {}
    for var in required_vars:
        value = os.getenv(var)
        env_status[var] = {
            "set": value is not None and value.strip() != "",
            "length": len(value) if value else 0
        }
    
    return {
        "status": "ok",
        "env_file_exists": env_exists,
        "environment_variables": env_status,
        "generator_initialized": _generator is not None and hasattr(_generator, "_initialized") and _generator._initialized
    }

@app.post("/chat")
def chat(request: ChatRequest, generator: ResponseGenerator = Depends(get_generator)):
    try:
        logger.info(f"Nhận được câu hỏi: {request.question[:100]}...")
        response = generator.generate_response(request.question)
        logger.info("Generate response thành công")
        return {"response": response}
    except HTTPException:
        # Re-raise HTTP exceptions
        raise
    except Exception as e:
        error_msg = str(e)
        error_trace = traceback.format_exc()
        logger.error(f"Lỗi khi generate response: {error_msg}")
        logger.error(f"Traceback: {error_trace}")
        
        # Provide more helpful error messages
        if "404" in error_msg or "Not Found" in error_msg:
            detail = f"Service không tìm thấy (404): {error_msg}. Kiểm tra QDRANT_URL hoặc HuggingFace API."
        elif "Connection" in error_msg or "refused" in error_msg.lower():
            detail = f"Không thể kết nối: {error_msg}. Kiểm tra QDRANT_URL hoặc network."
        elif "API" in error_msg or "token" in error_msg.lower():
            detail = f"Lỗi API hoặc authentication: {error_msg}. Kiểm tra API keys trong .env file."
        else:
            detail = f"Lỗi không mong muốn: {error_msg}"
        
        raise HTTPException(status_code=500, detail=detail)
