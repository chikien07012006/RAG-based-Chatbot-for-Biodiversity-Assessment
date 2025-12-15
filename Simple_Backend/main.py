from fastapi import FastAPI, Depends, HTTPException
from pydantic import BaseModel # Validate dữ liệU input/output
from langchain_core.messages import HumanMessage, AIMessage
from RAG_System import ResponseGenerator
from typing import List, Dict
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI()

response_generator = ResponseGenerator()

class Message(BaseModel):
    role: str  
    content: str
    
class ChatRequest(BaseModel):
    question: str
    
def get_generator():
    return response_generator

app.state.ready = False

@app.on_event("startup")
async def startup_event():
    logger.info("Khởi động ứng dụng ...")
    try:
        _ = ResponseGenerator()  
        logger.info("Khởi động thành công!")
        
        generator = ResponseGenerator()
        dummy_response = generator.generate_response(
            question="hi",
            chat_history=[]
        )
        logger.info(f"LLM hoàn tất. Dummy response length: {len(dummy_response)}")
        
        app.state.ready = True   
        logger.info("Hệ thống đã sẵn sàng phục vụ!")
    except Exception as e:
        logger.error(f"Lỗi khi warm-up: {str(e)}")
        
@app.get("/ready")
def ready_check():
    if app.state.ready:
        return {"ready": True, "message": "Sẵn sàng!"}
    else:
        raise HTTPException(status_code=503, detail="Đang khởi tạo hệ thống (warm-up model và dữ liệu)...")

@app.get("/")
def root():
    return {"status": "ok"}

@app.post("/chat")
def chat(request: ChatRequest, generator: ResponseGenerator = Depends(get_generator)):
    response = generator.generate_response(
    question=request.question
    )

    return {"response": response}
