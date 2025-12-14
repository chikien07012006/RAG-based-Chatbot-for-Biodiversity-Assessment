from langchain_huggingface import HuggingFaceEndpoint, ChatHuggingFace
from langchain_core.output_parsers import StrOutputParser
from langchain_core.runnables import RunnablePassthrough, RunnableLambda
from dotenv import load_dotenv
import os
from .retriever import Retriever  
from .prompt_template import prompt_template

load_dotenv()

class ResponseGenerator:
    _instance = None
    _initialized = False

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(ResponseGenerator, cls).__new__(cls)
        return cls._instance

    def __init__(self):
        if not self._initialized:
            self.retriever_obj = Retriever()

            self.retriever = RunnableLambda(
                lambda x: self.retriever_obj.get_content(x["question"])
            )
            
            self.prompt = prompt_template
            
            self.endpoint_llm = HuggingFaceEndpoint(
                repo_id="mistralai/Mistral-7B-Instruct-v0.2",  
                task="text-generation",
                max_new_tokens=512,
                temperature=0.7,
                repetition_penalty=1.03,
                do_sample=False,  
                huggingfacehub_api_token=os.getenv("HUGGINGFACEHUB_API_TOKEN")
            )
            
            self.llm = ChatHuggingFace(llm=self.endpoint_llm, verbose=False)

            self.chain = (
                {"context": self.retriever, "question": RunnablePassthrough()}
                | self.prompt
                | self.llm
                | StrOutputParser()
            )

            self._initialized = True

    def generate_response(self, question, chat_history: list = None):
        if chat_history is None:
            chat_history = []

        response = self.chain.invoke({
            "question": question,
        })

        return response
    
# # main.py hoặc app.py
# from fastapi import FastAPI, Request
# from pydantic import BaseModel
# from response_generator import ResponseGenerator
# from langchain_core.messages import HumanMessage, AIMessage

# app = FastAPI()

# # Khởi tạo generator khi app start (chỉ 1 lần)
# response_generator = ResponseGenerator()

# class ChatRequest(BaseModel):
#     question: str
#     chat_history: list[dict] = []  # optional, để client gửi history nếu muốn quản lý ở client-side

# @app.post("/chat")
# async def chat(request: ChatRequest):
#     # Convert chat_history từ dict sang LangChain messages nếu cần
#     history = []
#     for msg in request.chat_history:
#         if msg["role"] == "user":
#             history.append(HumanMessage(content=msg["content"]))
#         elif msg["role"] == "assistant":
#             history.append(AIMessage(content=msg["content"]))

#     response = response_generator.generate_response(
#         question=request.question,
#         chat_history=history
#     )

#     return {"response": response}