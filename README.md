# RAG-based Chatbot for Biodiversity Assessment (Hon Mun Coral Conservation)

This project implements a Retrieval-Augmented Generation (RAG) system designed to answer questions related to coral conservation in Hon Mun. It leverages a curated corpus of reports and news articles, advanced NLP processing for Vietnamese, and state-of-the-art LLMs to provide accurate and context-aware responses.

## 📖 Project Overview

The system is built to assist researchers and conservationists by retrieving relevant information from a specific knowledge base and generating human-like answers.

### Key Features
-   **Domain-Specific Knowledge**: Trained/Indexed on 50+ PDF reports and HTML news articles about Hon Mun coral conservation.
-   **Advanced Vietnamese NLP**: Utilizes `py_vncorenlp` for accurate text processing.
-   **High-Performance Retrieval**: Uses Qdrant vector database with Nomic embeddings and Cohere reranking for high precision.
-   **Modern LLM**: Powered by `mistralai/Mistral-7B-Instruct-v0.2`.
-   **Full-Stack Application**: Includes a FastAPI backend and a Streamlit frontend.

## ⚙️ System Architecture & Pipeline

### 1. Corpus Collection
-   **Source**: Web crawling of 50+ PDF reports and HTML news articles.
-   **Topic**: Coral conservation in Hon Mun.

### 2. Data Processing
-   **Cleaning**: Text cleaning using `py_vncorenlp` (specifically for Vietnamese language nuances) and `NLTK`.
-   **Chunking**: Data is split into manageable chunks using LangChain's `RecursiveCharacterTextSplitter`.
-   **Embedding**: Chunks are converted into vector embeddings using the `nomic-ai/nomic-embed-text-v1.5` model.

### 3. Vector Database
-   **Storage**: 2408 text chunks stored in a **Qdrant** cloud vector database.

### 4. Retrieval & Generation (RAG)
-   **Retrieval**: `similarity_search_by_vector` is used to find relevant chunks.
-   **Reranking**: Results are refined using **Cohere's `rerank-multilingual-v3.0`** to ensure the most relevant context is prioritized.
-   **Generation**: The context is fed into **`mistralai/Mistral-7B-Instruct-v0.2`** (via Hugging Face) to generate the final answer.

## 🛠️ Tech Stack

-   **Language**: Python
-   **Frameworks**: FastAPI (Backend), Streamlit (Frontend)
-   **Orchestration**: LangChain
-   **NLP & AI**:
    -   `py_vncorenlp`, `NLTK`
    -   Hugging Face (Embeddings & LLM)
    -   Cohere (Reranking)
-   **Database**: Qdrant (Vector DB)

## 📂 Project Structure

```
RAG_for_Biodiversity_Assessment/
├── DATA/                     # Raw corpus (PDFs, HTMLs)
├── RAG_System/               # Core logic for RAG pipeline
│   ├── Data_Preprocessing_and_VectorDB/
│   └── Retrieval/
├── Simple_Backend/           # FastAPI application
├── Simple_Frontend/          # Streamlit application
├── VnCoreNLP_Model/          # Vietnamese NLP model files
├── .env                      # Environment variables
├── requirements.txt          # Python dependencies
└── README.md                 # Project documentation
```

## 🚀 Getting Started

### Prerequisites
-   Python 3.8+
-   Qdrant Cloud Account
-   Hugging Face API Token
-   Cohere API Key (if using Rerank directly via API, though often handled via LangChain wrappers)

### Installation

1.  **Clone the repository**
    ```bash
    git clone <repo-url>
    cd RAG_for_Biodiversity_Assessment
    ```

2.  **Install Dependencies**
    ```bash
    pip install -r requirements.txt
    ```

3.  **Environment Setup**
    -   Create a `.env` file based on `env.example`.
    -   Fill in your API keys and configuration:
        ```ini
        HUGGINGFACEHUB_API_TOKEN=your_token
        QDRANT_URL=your_qdrant_url
        QDRANT_API_KEY=your_qdrant_key
        QDRANT_COLLECTION=your_collection_name
        # ... other vars
        ```

### Running the Application

1.  **Start the Backend (FastAPI)**
    ```bash
    cd Simple_Backend
    uvicorn app:app --reload
    # OR if configured in root
    # python -m uvicorn Simple_Backend.app:app --reload
    ```

2.  **Start the Frontend (Streamlit)**
    Open a new terminal:
    ```bash
    cd Simple_Frontend
    streamlit run app.py
    ```

3.  **Access the App**
    -   Frontend: `http://localhost:8501`
    -   Backend Docs: `http://localhost:8000/docs`
