import importlib
import subprocess
import sys

# Danh sách thư viện cần
required_libs = [
    "os",  # built-in, sẽ không cần cài
    "pickle",  # built-in
    "google_auth_oauthlib.flow",
    "google.auth.transport.requests"
]

def install_if_missing(package, import_name=None):
    try:
        importlib.import_module(import_name or package)
    except ImportError:
        print(f"Cài đặt thư viện {package}...")
        subprocess.check_call([sys.executable, "-m", "pip", "install", package])

# Mapping: package trên pip -> module import
packages_map = {
    "google-auth-oauthlib": "google_auth_oauthlib.flow",
    "google-auth": "google.auth.transport.requests"
}

for pip_name, module_name in packages_map.items():
    install_if_missing(pip_name, module_name)

# Sau khi đảm bảo đã cài, import bình thường
import os
import pickle
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request

# Thư mục credentials và token
CREDENTIALS_FOLDER = "credentials"
TOKEN_FOLDER = "token"
os.makedirs(TOKEN_FOLDER, exist_ok=True)

# Scope API
SCOPES = [
    "https://www.googleapis.com/auth/yt-analytics.readonly",
    "https://www.googleapis.com/auth/youtube.readonly",
]

def create_token(cred_file):
    cred_path = os.path.join(CREDENTIALS_FOLDER, cred_file)
    token_path = os.path.join(TOKEN_FOLDER, os.path.splitext(cred_file)[0] + ".pickle")

    creds = None
    if os.path.exists(token_path):
        with open(token_path, "rb") as f:
            creds = pickle.load(f)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(cred_path, SCOPES)
            creds = flow.run_local_server(port=0)  # mở trình duyệt mặc định
        with open(token_path, "wb") as f:
            pickle.dump(creds, f)
    print(f"✓ Token đã lưu: {token_path}")

if __name__ == "__main__":
    files = [f for f in os.listdir(CREDENTIALS_FOLDER) if f.endswith(".json")]
    if not files:
        print("Không có file credentials trong thư mục.")
    else:
        for f in files:
            create_token(f)
