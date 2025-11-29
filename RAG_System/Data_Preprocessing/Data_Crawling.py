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
    "https://iucn.org/sites/default/files/2022-05/hon_mun_mid_term_eval_final.pdf",
    "https://archive.iwlearn.org/unepscs.org/www.unepscs.org/Economic_Valuation_Training_Materials/06%20Readings%20on%20Economic%20Valuation%20of%20Coastal%20Habitats/20-Recreation-Value-Coral-Reefs-Hon-Mun-Island-Vietnam.pdf",
    "https://e-services.nafosted.gov.vn/upload/pub_certificates/pub_31218_92862.pdf",
    "https://iucn.org/sites/default/files/import/downloads/iucn_coral_reef_portfolio.pdf",
    "https://gcrmn.net/wp-content/uploads/2023/01/Status-of-Coral-Reefs-of-the-World-2020-Full-Report.pdf",
    "https://www.coraltriangleinitiative.org/sites/default/files/resources/regional-state-coral-triangle_0.pdf",
    "https://gefcoral.org/Portals/53/downloads/disease_products/0807%20Coral%20disease%20handbook%20sample.pdf",
    "https://www.biosphere-expeditions.org/images/stories/pdfs/reefcheck/InstructionManual16.pdf"
    "http://mrc.gov.mv/assets/Uploads/1-MCBRP-2017New.pdf",
    "https://www.coraltriangleinitiative.org/sites/default/files/resources/LEAP_Final_complete.pdf",
    "https://reefresilience.org/pdf/coral_reef_resilience_gg-rs.pdf",

    # HTML news web
    "https://nhandan.vn/hoi-sinh-ran-san-ho-hon-mun-post860078.html",
    "https://vnexpress.net/du-khach-phat-hien-ca-chet-hang-loat-san-ho-gay-rap-duoi-bien-khanh-hoa-4929958.html",
    "https://vnexpress.net/gan-200-ha-san-ho-bien-mat-o-vinh-nha-trang-4911314.html",
    "https://vasi.mae.gov.vn/cuoc-thi-cung-giu-mau-xanh-cua-bien/bao-ton-he-sinh-thai-san-ho--bai-1-vai-tro-cua-he-sinh-thai-san-ho-906.htm",
    "https://vasi.mae.gov.vn/tin-tuc--su-kien/bao-ve-da-dang-sinh-hoc-nhan-len-su-song-trong-long-bien-1883.htm",
    "https://nhandan.vn/bao-ton-phat-trien-he-sinh-thai-vung-bien-dao-truong-sa-post218227.html",
    "https://nongnghiepmoitruong.vn/bao-ton-he-sinh-thai-san-ho--rung-mua-nhiet-doi-duoi-bien-d339279.html",
    "https://vasi.mae.gov.vn/cuoc-thi-cung-giu-mau-xanh-cua-bien/bao-ton-he-sinh-thai-ran-san-ho--bai-2-ran-san-ho-keu-cuu-905.htm",
    "https://baokhanhhoa.vn/xa-hoi/202509/chaomungdai-hoi-dai-bieu-dang-bo-tinh-khanh-hoa-lan-thu-i-nhiem-ky-2025-2030-bao-ton-phat-huy-gia-tri-ben-vung-he-sinh-thai-bienvinh-nha-trang-e4914ab/",
    "https://baokhanhhoa.vn/du-lich/201706/bao-ve-ran-san-ho-o-hon-mun-truoc-tien-la-y-thuc-cua-huong-dan-vien-8045182/",
    "https://tapchimoitruong.vn/chuyen-muc-3/kinh-nghiem-bao-ve-moi-truong-di-san-thien-nhien-tai-ran-san-ho-great-barrier-cua-oxtraylia-va-bai-hoc-cho-viet-nam-30294",
    "https://nhandan.vn/ran-san-ho-trang-khong-lo-post914877.html",
    "https://nhandan.vn/cong-dong-dia-phuong-tham-gia-phuc-hoi-ran-san-ho-post911657.html",
    "https://nhandan.vn/hon-80-ran-san-ho-toan-cau-bi-anh-huong-sau-dot-tay-trang-lon-nhat-lich-su-post874704.html",
    "https://en.nhandan.vn/reviving-hon-muns-coral-reefs-post144454.html",
    "https://www.coris.noaa.gov/activities/reef_managers_guide/",
    "https://www.mdpi.com/1424-2818/17/6/434",
    "https://www.unep.org/resources/report/coral-bleaching-futures",
    "https://www.mdpi.com/2079-9292/13/24/5027",
    "https://arxiv.org/pdf/2405.14879",
    "https://www.mdpi.com/2072-4292/12/3/489",
    "https://www.digilab.ai/news/digilab-developing-world-first-ai-for-planet%E2%80%99s-largest-coral-reef-restoration-programme"
    
    
    
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