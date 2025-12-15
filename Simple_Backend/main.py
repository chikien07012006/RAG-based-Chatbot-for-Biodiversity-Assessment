from fastapi import FastAPI, Depends, HTTPException
from pydantic import BaseModel
from RAG_System import ResponseGenerator
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI()

class ChatRequest(BaseModel):
    question: str

# Singleton nhưng LAZY
_generator = None

def get_generator():
    global _generator
    if _generator is None:
        logger.info("Khởi tạo ResponseGenerator (lazy)...")
        _generator = ResponseGenerator()
    return _generator

@app.get("/")
def root():
    return {"status": "ok"}

@app.get("/ready")
def ready():
    return {"ready": True}

@app.post("/chat")
def chat(request: ChatRequest, generator: ResponseGenerator = Depends(get_generator)):
    try:
        response = generator.generate_response(request.question)
        return {"response": response}
    except Exception as e:
        logger.exception("Lỗi khi generate response")
        raise HTTPException(status_code=500, detail=str(e))
