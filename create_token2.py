import importlib
import subprocess
import sys
import os
import pickle
import threading
import traceback
from tkinter import Tk, Button, Label, Listbox, MULTIPLE, END, filedialog, Scrollbar, RIGHT, Y, LEFT, BOTH, Checkbutton, IntVar, StringVar, Frame, W
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request

def install_if_missing(package, import_name=None):
    try:
        importlib.import_module(import_name or package)
    except ImportError:
        subprocess.check_call([sys.executable, "-m", "pip", "install", package])

packages_map = {
    "google-auth-oauthlib": "google_auth_oauthlib.flow",
    "google-auth": "google.auth.transport.requests",
}
for pip_name, mod_name in packages_map.items():
    install_if_missing(pip_name, mod_name)

SCOPES = [
    "https://www.googleapis.com/auth/yt-analytics.readonly",
    "https://www.googleapis.com/auth/youtube.readonly",
]

# ---------- Core Logic ----------
def list_credential_files(folder: str):
    try:
        return [f for f in os.listdir(folder) if f.lower().endswith(".json")]
    except Exception:
        return []

def create_token_for_file(folder: str, cred_file: str, log_fn=print):
    cred_path = os.path.join(folder, cred_file)
    token_path = os.path.join(folder, os.path.splitext(cred_file)[0] + ".pickle")

    log_fn(f"→ Đang xử lý: {cred_file}")
    creds = None

    if os.path.exists(token_path):
        try:
            with open(token_path, "rb") as f:
                creds = pickle.load(f)
        except Exception as e:
            log_fn(f"  ! Không thể đọc token cũ: {e}. Sẽ tạo lại.")

    try:
        if not creds or not getattr(creds, "valid", False):
            if creds and getattr(creds, "expired", False) and getattr(creds, "refresh_token", None):
                creds.refresh(Request())
                log_fn("  ✓ Token đã refresh.")
            else:
                flow = InstalledAppFlow.from_client_secrets_file(cred_path, SCOPES)
                creds = flow.run_local_server(port=0)
                log_fn("  ✓ Đã xác thực OAuth.")
        with open(token_path, "wb") as f:
            pickle.dump(creds, f)
        log_fn(f"  ✓ Token đã lưu: {os.path.basename(token_path)}")
        return True
    except Exception as e:
        log_fn(f"  ✗ Lỗi tạo token: {e}")
        log_fn(traceback.format_exc())
        return False

# ---------- GUI ----------
class TokenGUI:
    def __init__(self, root):
        self.root = root
        self.root.title("YouTube OAuth Token Creator")
        self.root.geometry("700x520")

        self.selected_folder = StringVar(value="")
        self.select_all_var = IntVar(value=1)

        # Folder chooser
        top_frame = Frame(root)
        top_frame.pack(padx=10, pady=10, anchor=W, fill="x")

        Label(top_frame, text="Credentials folder:").pack(side=LEFT)
        self.lbl_folder = Label(top_frame, textvariable=self.selected_folder, fg="#333", anchor="w")
        self.lbl_folder.pack(side=LEFT, padx=6, expand=True, fill="x")

        self.btn_browse = Button(top_frame, text="Browse...", command=self.browse_folder)
        self.btn_browse.pack(side=LEFT, padx=5)

        # Listbox + scrollbar
        mid_frame = Frame(root)
        mid_frame.pack(padx=10, pady=(0, 10), fill=BOTH, expand=True)

        Label(mid_frame, text="Chọn file .json để tạo token:").pack(anchor=W)

        list_frame = Frame(mid_frame)
        list_frame.pack(fill=BOTH, expand=True)

        self.listbox = Listbox(list_frame, selectmode=MULTIPLE, height=12)
        self.listbox.pack(side=LEFT, fill=BOTH, expand=True)

        scrollbar = Scrollbar(list_frame)
        scrollbar.pack(side=RIGHT, fill=Y)
        self.listbox.config(yscrollcommand=scrollbar.set)
        scrollbar.config(command=self.listbox.yview)

        # Select all checkbox
        self.chk_all = Checkbutton(mid_frame, text="Select all", variable=self.select_all_var, command=self.toggle_select_all)
        self.chk_all.pack(anchor=W, pady=(6, 0))

        # Buttons
        btn_frame = Frame(root)
        btn_frame.pack(padx=10, pady=10, anchor=W)

        self.btn_create = Button(btn_frame, text="Create Tokens", command=self.start_create_tokens)
        self.btn_create.pack(side=LEFT)

        self.btn_refresh = Button(btn_frame, text="Reload List", command=self.reload_files)
        self.btn_refresh.pack(side=LEFT, padx=6)

        # Log area
        Label(root, text="Logs:").pack(anchor=W, padx=10)
        self.log_list = Listbox(root, height=10)
        self.log_list.pack(padx=10, pady=(0, 10), fill=BOTH, expand=False)

        # Init
        self.disable_controls(True)
        self.browse_folder()  # Prompt immediately

    def disable_controls(self, busy: bool):
        state = "disabled" if busy else "normal"
        self.btn_browse.config(state=state)
        self.btn_create.config(state=state)
        self.btn_refresh.config(state=state)
        self.listbox.config(state=state)
        self.chk_all.config(state=state)

    def browse_folder(self):
        folder = filedialog.askdirectory(title="Chọn thư mục chứa credentials (.json)")
        if folder:
            self.selected_folder.set(folder)
            self.reload_files()
            self.disable_controls(False)
        else:
            # No folder selected yet; keep disabled until a folder is chosen
            self.disable_controls(False if self.selected_folder.get() else True)

    def reload_files(self):
        self.listbox.delete(0, END)
        folder = self.selected_folder.get()
        files = list_credential_files(folder)
        for f in files:
            self.listbox.insert(END, f)
        # Auto-select all if checkbox is ticked
        if self.select_all_var.get():
            self.listbox.select_set(0, END)

    def toggle_select_all(self):
        if self.select_all_var.get():
            self.listbox.select_set(0, END)
        else:
            self.listbox.select_clear(0, END)

    def log(self, msg: str):
        self.log_list.insert(END, msg)
        self.log_list.see(END)
        self.root.update_idletasks()

    def start_create_tokens(self):
        folder = self.selected_folder.get()
        if not folder:
            self.log("✗ Chưa chọn thư mục.")
            return

        # Determine selections
        selections = self.listbox.curselection()
        if not selections:
            self.log("✗ Chưa chọn file nào.")
            return

        files = [self.listbox.get(i) for i in selections]
        self.disable_controls(True)

        def worker():
            ok = 0
            for f in files:
                if create_token_for_file(folder, f, log_fn=self.log):
                    ok += 1
            self.log(f"— Hoàn tất: {ok}/{len(files)} token thành công.")
            self.disable_controls(False)

        threading.Thread(target=worker, daemon=True).start()

# ---------- Main ----------
if __name__ == "__main__":
    root = Tk()
    app = TokenGUI(root)
    root.mainloop()
