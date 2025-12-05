from langchain_community.document_loaders import (
    PyPDFLoader,         # .pdf
    UnstructuredHTMLLoader # .html
    # Docx2txtLoader,      # .docx
    # UnstructuredWordDocumentLoader,  # .doc cũ
    # UnstructuredExcelLoader,         # .xlsx, .xls
    # UnstructuredPowerPointLoader,    # .pptx
    # TextLoader,          # .txt
    # CSVLoader,          # .csv
)
from langchain_text_splitters import RecursiveCharacterTextSplitter
from langdetect import detect, DetectorFactory
from pathlib import Path
import nltk
import re # regular expression
import py_vncorenlp

DetectorFactory.seed = 0

nltk.download('all')  

def load_all_documents(data_folder):
    docs = []
    folder = Path(data_folder)
    for file_path in folder.rglob("*.*"):
        file_path = str(file_path)
        loader = None
        if file_path.lower().endswith(".pdf"):
            loader = PyPDFLoader(file_path)
        elif file_path.lower().endswith(".html"):
            loader = UnstructuredHTMLLoader(file_path)
        else:
            continue
            
        docs.extend(loader.load())
        
    return docs

py_vncorenlp.download_model(save_dir='D:\RAG for Biodiversity Assessment')
vncorenlp = py_vncorenlp.VnCoreNLP(
    save_dir = "D:\RAG_for_Biodiversity_Assessment\VnCoreNLP_Model",
    annotators=["wseg", "pos", "ner"], 
)

def is_vietnamese(text):
    try:
        sample = text.replace("\n", " ")[:200]
        lang = detect(sample)
        return lang == "vi"
    except:
        return False

def clean_text(text):
    if not text or len(text.strip()) < 20: # <20 ký tự thường là văn bản trích xuất lỗi hoặc dữ liệu 0 có ý nghĩa
        return ""
        
    
    lines = text.splitlines()
    cleaned_lines = []
    for line in lines:
        line = line.strip()
        if not line:
            continue
            
        noise_patterns = [
            r"^Trang\s*\d+\/?\d*\s*$", 
            r"^\d+\s*$",  
            r"^[\-\_]{3,}$", 
            r"^Confidential.*$", r"^Nội bộ.*$", r"^MẬT.*$",
            r"^CÔNG TY.*$", r"^CỘNG HÒA XÃ HỘI.*VIỆT NAM$",
            r"^Độc lập - Tự do - Hạnh phúc$",
            r"^Lời cảm ơn.*$", r"^Mục lục$", r"^Table of Contents$",
            r"^Chữ ký.*$", r"^ĐẠI DIỆN.*$", r"^Người lập.*$",
            r"^Họ và tên.*$", r"^Ký tên.*$",
            r'^Page\s*\d+\s*(of\s*\d+)?$',           
            r'^\d+\s*/\s*\d+$',                    
            r'^\d+$',                             
            r'^[─\-_=—]{4,}$',                       
            r'^Confidential\s*[-–—]*\s*\d*$', 
            r'^Proprietary\s+and\s+Confidential$',
            r'^For\s+Internal\s+Use\s+Only$',
            r'^DRAFT.*$',
            r'^Table\s+of\s+Contents$',
            r'^Contents$',
            r'^Figure\s+\d+.*$',
            r'^Table\s+\d+.*$',
            r'^\s*Copyright\s+©?\s*\d{4}.*$',
            r'^\s*©.*\d{4}.*$',
            r'^\s*All\s+Rights\s+Reserved.*$',
            r'^www\..*$',
            r'^\S+@\S+\.\S+$',
        ]
        
        if any(re.match(p, line, flags=re.IGNORECASE) for p in noise_patterns):
            continue
        
        signature_patterns = [
            r'^Signature.*$',
            r'^Signed.*$',
            r'^Name\s*[:–-]\s*.+$',
            r'^Title\s*[:–-]\s*.+$',
            r'^Date\s*[:–-]\s*.+$',
            r'^Prepared\s+by.*$',
            r'^Reviewed\s+by.*$',
            r'^Approved\s+by.*$',
            r'^Author.*$',
            r'^Version.*$',
        ]
        if any(re.search(p, line, flags=re.IGNORECASE) for p in signature_patterns):
            continue
            
        if re.fullmatch(r"[\w\.\-]+@[\w\.\-]+\.[\w]+", line):
            continue
        if re.fullmatch(r"\d{2,4}[\/\-\.\s]\d{1,2}([\/\-\.\s]\d{1,4})?", line):
            continue
            
        cleaned_lines.append(line)
    
    text = "\n".join(cleaned_lines)
    
    text = re.sub(r"\s+", " ", text)
    text = re.sub(r'[ \t]+', ' ', text)            
    text = re.sub(r'\s+\n', '\n', text)         
    text = re.sub(r'\n+', '\n', text)               
    text = text.strip() + "\n"
    
    if is_vietnamese(text):
        segmented = vncorenlp.word_segment(text)
        text = " ".join(segmented)   
    
    words = text.split()
    if len(words) < 10:
        return ""
        
    return text.strip()

Chunker = RecursiveCharacterTextSplitter(
    chunk_size=1000,      
    chunk_overlap=100,
    separators=["\n\n", "\n", ". ", "! ", "? ", " "],
    keep_separator=True,
    length_function=len,
)

def full_pipeline_Cleaning_and_Chunking(data_folder):
    raw_data = load_all_documents(data_folder)
    
    cleaned_docs = []
    for doc in raw_data:
        cleaned_doc = clean_text(doc.page_content)
        cleaned_docs.append(cleaned_doc)
    
    # Chunking
    final_docs = Chunker.split_documents(cleaned_docs)
    
    return final_docs