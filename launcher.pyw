import os
import socket
import sys
import threading
import time
import traceback
import webbrowser

import uvicorn


APP_NAME = "Truck Load Finder"
HOST = "127.0.0.1"
PORT = 8000


def resource_path(relative_path: str) -> str:
    base = getattr(sys, "_MEIPASS", os.path.dirname(os.path.abspath(__file__)))
    return os.path.join(base, relative_path)


def app_data_dir() -> str:
    base = os.getenv("APPDATA") or os.path.expanduser("~")
    return os.path.join(base, APP_NAME)


def ensure_app_data_dir() -> str:
    path = app_data_dir()
    os.makedirs(path, exist_ok=True)
    return path


def is_port_in_use(host: str, port: int) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            sock.bind((host, port))
        except OSError:
            return True
    return False


def show_error(message: str) -> None:
    try:
        import tkinter as tk
        from tkinter import messagebox

        root = tk.Tk()
        root.withdraw()
        messagebox.showerror(APP_NAME, message)
        root.destroy()
    except Exception:
        pass


def open_browser_later(url: str, delay_seconds: float = 1.5) -> None:
    time.sleep(delay_seconds)
    webbrowser.open(url)


def write_error_log(message: str) -> str:
    data_dir = ensure_app_data_dir()
    log_path = os.path.join(data_dir, "error.log")
    with open(log_path, "a", encoding="utf-8") as handle:
        handle.write(message)
        if not message.endswith("\n"):
            handle.write("\n")
    return log_path


def main() -> None:
    try:
        if is_port_in_use(HOST, PORT):
            show_error(
                f"{APP_NAME} could not start because port {PORT} is already in use. "
                "Close the other app using that port and try again."
            )
            return

        data_dir = ensure_app_data_dir()
        os.environ["LOADS_DB_PATH"] = os.path.join(data_dir, "loads.db")
        os.environ["SAMPLE_LOADS_PATH"] = resource_path(os.path.join("data", "sample_loads.json"))

        browser_thread = threading.Thread(
            target=open_browser_later,
            args=(f"http://{HOST}:{PORT}/",),
            daemon=True,
        )
        browser_thread.start()

        uvicorn.run(
            "src.main:app",
            host=HOST,
            port=PORT,
            log_config=None,
            log_level="warning",
        )
    except Exception:
        trace = traceback.format_exc()
        log_path = write_error_log(trace)
        show_error(
            f"{APP_NAME} failed to start.\n\n"
            f"Details were written to:\n{log_path}"
        )


if __name__ == "__main__":
    main()
