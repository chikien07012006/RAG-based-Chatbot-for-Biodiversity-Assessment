import os
import requests # Download file from internet
from urllib.parse import urlparse
import hashlib # Create unique ID
import time
import json # Save Metadata
from bs4 import BeautifulSoup # Read content from HTML
from tqdm import tqdm

BASE_DIR = "DATA\Raw"
os.makedirs(BASE_DIR, exist_ok=True)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "vi-VN,vi;q=0.9,en;q=0.8",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
} # giữ header vì nếu xoá thì 1 số link sẽ bị chặn như baokhanhhoa.vn, khanhhoa.gov.vn, ... vì nghĩ người cào là bot

def safe_filename(url): # Create unique file name from url
    parsed = urlparse(url)
    name = os.path.basename(parsed.path).split("?")[0]  # bỏ query string
    if not name or "." not in name:
        name = hashlib.md5(url.encode()).hexdigest()[:15]
    return name

def get_extension(url, content_type):  # Xác định đuôi file
    if "pdf" in content_type:
        return ".pdf"
    if "msword" in content_type or "officedocument" in content_type:
        return ".docx"
    if url.endswith((".pdf", ".PDF")):
        return ".pdf"
    if url.endswith((".docx", ".doc")):
        return ".docx"
    return ".html"

def download_and_save_with_metadata(url, subfolder="others_types"):
    try:
        save_dir = os.path.join(BASE_DIR, subfolder)
        os.makedirs(save_dir, exist_ok=True)

        # Tải file
        r = requests.get(url, headers=HEADERS, timeout=60, stream=True) # Tải dữ liệu từ URL, headers=HEADERS Giả lập trình duyệt (compulsory)
        r.raise_for_status()

        # Xác định đuôi file 
        content_type = r.headers.get("Content-Type", "")
        ext = get_extension(url, content_type)
        
        # Xác định tên file an toàn để lưu về local
        base_name = safe_filename(url)
        if not base_name.lower().endswith(ext.lower()):
            base_name += ext

        file_path = os.path.join(save_dir, base_name)
        meta_path = file_path + "_meta.json"

        # Bỏ qua nếu đã tồn tại file đó 
        if os.path.exists(file_path) and os.path.exists(meta_path):
            return file_path

        # Lưu file gốc, lưu file xuống ổ cứng, chia nhỏ ra và lưu, không làm đầy RAM
        with open(file_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)

        # Tạo metadata
        metadata = {
            "source_url": url,
            "crawl_date": time.strftime("%Y-%m-%d"),
            "file_type": ext[1:],
            "file_size_kb": round(os.path.getsize(file_path) / 1024, 1),
            "content_hash": hashlib.md5(r.content).hexdigest(),
            "domain": urlparse(url).netloc,
        }

        # Nếu là HTML → trích xuất thêm title, description, author...
        if ext == ".html":
            try:
                soup = BeautifulSoup(r.text, "lxml")
                title = soup.find("title") # Lấy tiêu đề
                metadata["title"] = title.text.strip() if title else ""
                desc = soup.find("meta", {"name": "description"}) or soup.find("meta", {"property": "og:description"}) # Lấy Phần mô tả
                metadata["description"] = desc["content"] if desc and desc.get("content") else ""
                pub = soup.find("meta", {"property": "article:published_time"}) or soup.find("meta", {"name": "pubdate"}) or soup.find("time", {"class": "time"}) or soup.find("span", {"class": "date"})  # Lấy ngày đăng bài 
                metadata["publish_date"] = pub["content"][:10] if pub and pub.get("content") else ""
                author = soup.find("div", {"class": "article__author"})
                metadata["author"] = author["content"] if author else ""
            except:
                pass
        else:
            metadata["title"] = base_name

        # Lưu metadata
        with open(meta_path, "w", encoding="utf-8") as f:
            json.dump(metadata, f, ensure_ascii=False, indent=2)

        return file_path

    except Exception as e:
        print(f"Failed {url} → {e}")
        return None


URLS = [
    # PDF khoa học & báo cáo
    "",
    

    # HTML news web
    "https://nhandan.vn/hoi-sinh-ran-san-ho-hon-mun-post860078.html",
    "https://vnexpress.net/du-khach-phat-hien-ca-chet-hang-loat-san-ho-gay-rap-duoi-bien-khanh-hoa-4929958.html",
    "https://vnexpress.net/gan-200-ha-san-ho-bien-mat-o-vinh-nha-trang-4911314.html"  

    # docsx, policies, ...
    
    
]
 
 
for url in tqdm(URLS, desc="Tổng tiến độ"):
    if url.endswith((".pdf", ".PDF")):
        download_and_save_with_metadata(url, "PDF")
    elif url.endswith((".docx", ".doc")):
        download_and_save_with_metadata(url, "DOCX")
    else:
        download_and_save_with_metadata(url, "HTML")
    time.sleep(0.6)  

print(f"\nDone, all saved in{os.path.abspath(BASE_DIR)}")