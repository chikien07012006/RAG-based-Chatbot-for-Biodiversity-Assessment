import streamlit as st
import requests
import os
from dotenv import load_dotenv
import time

# Load environment variables from .env file
dotenv_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), '.env')
load_dotenv(dotenv_path=dotenv_path)

st.set_page_config(
    page_title="Chatbot Rạn San Hô Việt Nam",
    page_icon="🌊",
    layout="centered"
)

BACKEND_URL = os.getenv("BACKEND_ROOT_URL") 

def is_backend_ready():
    try:
        response = requests.get(f"{BACKEND_URL}/ready", timeout=5)
        return response.status_code == 200
    except:
        return False

if not is_backend_ready():
    st.markdown("<h1 style='text-align: center;'>🌊 Chatbot Rạn San Hô Việt Nam</h1>", unsafe_allow_html=True)
    st.markdown("<h3 style='text-align: center; color: #666;'>Đang khởi tạo hệ thống...</h3>", unsafe_allow_html=True)
    
    col1, col2, col3 = st.columns([1, 2, 1])
    with col2:
        with st.spinner("Đang load dữ liệu và khởi động AI (có thể mất 10-30 giây lần đầu)..."):
            time.sleep(3)  
    st.rerun()  
else:
    st.title("🌊 Chatbot Kiến Thức Bảo Tồn Rạn San Hô Việt Nam")
    st.markdown("Hỏi tôi về hệ sinh thái san hô, tình trạng tẩy trắng, bảo tồn tại Hòn Mun, Trường Sa, Nha Trang...")

    if "messages" not in st.session_state:
        st.session_state.messages = []

    for message in st.session_state.messages:
        with st.chat_message(message["role"]):
            st.markdown(message["content"])

    if prompt := st.chat_input("Nhập câu hỏi của bạn..."):
        st.session_state.messages.append({"role": "user", "content": prompt})
        with st.chat_message("user"):
            st.markdown(prompt)

        payload = {
            "question": prompt
        }

        with st.chat_message("assistant"):
            with st.spinner("Retrieving answer..."): 

                try:
                    response = requests.post(f"{BACKEND_URL}/chat", json=payload, timeout=60)
                    if response.status_code == 200:
                        answer = response.json().get("response", "Không nhận được phản hồi.")
                    else:
                        answer = f"Lỗi từ server: {response.status_code} - {response.text}"
                except requests.exceptions.Timeout:
                    answer = "Timeout: Phản hồi từ model quá chậm, vui lòng thử lại."
                except requests.exceptions.ConnectionError:
                    answer = "Không thể kết nối đến backend. Kiểm tra URL hoặc server đang chạy chưa."
                except Exception as e:
                    answer = f"Lỗi không mong muốn: {str(e)}"

            st.markdown(answer)

            st.session_state.messages.append({"role": "assistant", "content": answer})

    st.caption("Dữ liệu dựa trên các tài liệu khoa học, báo chí và nghiên cứu về bảo tồn rạn san hô tại Việt Nam (2020-2025).")