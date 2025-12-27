from langchain_huggingface import HuggingFaceEndpoint, ChatHuggingFace
from langchain_core.output_parsers import StrOutputParser
from langchain_core.runnables import RunnablePassthrough, RunnableLambda
from dotenv import load_dotenv
import os
import logging
from .retriever import Retriever  
from .prompt_template import prompt_template

# Load environment variables from .env file
dotenv_path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), '.env')
load_dotenv(dotenv_path=dotenv_path)
logger = logging.getLogger(__name__)

class ResponseGenerator:
    def __init__(self):
        self._initialized = False

    def _init_chain(self):
        if self._initialized:
            return

        try:
            logger.info("Khởi tạo Retriever...")
            self.retriever_obj = Retriever()

            self.retriever = RunnableLambda(
                lambda x: self.retriever_obj.get_content(x["question"])
            )

            self.prompt = prompt_template

            huggingface_token = os.getenv("HUGGINGFACEHUB_API_TOKEN")
            print(huggingface_token)
            if not huggingface_token:
                raise ValueError("HUGGINGFACEHUB_API_TOKEN environment variable is not set")
            
            logger.info("Khởi tạo HuggingFace LLM endpoint...")
            self.endpoint_llm = HuggingFaceEndpoint(
                repo_id="NousResearch/Hermes-2-Pro-Mistral-7B",
                task="text-generation",
                max_new_tokens=512,
                temperature=0.7,
                huggingfacehub_api_token=huggingface_token
            )

            self.llm = ChatHuggingFace(llm=self.endpoint_llm)

            self.chain = (
                {"context": self.retriever, "question": RunnablePassthrough()}
                | self.prompt
                | self.llm
                | StrOutputParser()
            )

            logger.info("ResponseGenerator chain khởi tạo thành công")
            self._initialized = True
        except Exception as e:
            logger.error(f"Lỗi khi khởi tạo chain: {str(e)}")
            raise

    def initialize(self):
        """Explicitly initialize the chain and models"""
        self._init_chain()

    def generate_response(self, question):
        try:
            if not self._initialized:
                self._init_chain()

            logger.debug(f"Đang generate response cho câu hỏi: {question[:50]}...")
            response = self.chain.invoke({"question": question})
            return response
        except Exception as e:
            logger.error(f"Lỗi trong generate_response: {str(e)}")
            raise
