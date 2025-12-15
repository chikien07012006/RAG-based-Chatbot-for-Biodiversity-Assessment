from langchain_huggingface import HuggingFaceEndpoint, ChatHuggingFace
from langchain_core.output_parsers import StrOutputParser
from langchain_core.runnables import RunnablePassthrough, RunnableLambda
from dotenv import load_dotenv
import os
from .retriever import Retriever  
from .prompt_template import prompt_template

load_dotenv()

class ResponseGenerator:
    def __init__(self):
        self._initialized = False

    def _init_chain(self):
        if self._initialized:
            return

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
            huggingfacehub_api_token=os.getenv("HUGGINGFACEHUB_API_TOKEN")
        )

        self.llm = ChatHuggingFace(llm=self.endpoint_llm)

        self.chain = (
            {"context": self.retriever, "question": RunnablePassthrough()}
            | self.prompt
            | self.llm
            | StrOutputParser()
        )

        self._initialized = True

    def generate_response(self, question):
        if not self._initialized:
            self._init_chain()

        return self.chain.invoke({"question": question})
