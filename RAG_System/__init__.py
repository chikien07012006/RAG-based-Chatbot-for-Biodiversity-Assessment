# """
# rag_chatbot - A professional RAG-based chatbot using LangChain, Gemini, Qdrant...

# Author: Your Name
# Version: 0.1.0
# """

# # Version thông tin (rất hữu ích cho logging/debug)
# __version__ = "0.1.0"

# # Explicit public API - chỉ expose những gì user cần dùng trực tiếp
# __all__ = [
#     "RagChatbot",          # Class chính nếu có
#     "get_retriever",       # Factory functions
#     "get_rag_chain",
#     "settings",            # Config singleton
#     "embed_document",      # Common functions
# ]

# # Import public interfaces từ sub-modules (explicit > implicit)
# from .config.settings import settings
# from .retrieval.retriever import get_retriever
# from .generation.rag_chain import get_rag_chain, RagChain
# from .ingestion.pipeline import ingest_documents  # ví dụ

# # Optional: Package-level init code (logging, warnings...)
# import logging

# logging.getLogger(__name__).addHandler(logging.NullHandler())

# # Ví dụ: lazy init nếu cần heavy stuff
# # def _init_package():
# #     # connect to Qdrant once, etc.
# #     pass
# # _init_package()