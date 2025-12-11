# OverlayClientGUI.py
# GUI inspirada no ClientGUI do ZIP, mas lê frames cache-<seq>.jpg
# a partir de cache/<client_name>/<stream_id>/

import os
import argparse
import tkinter as tk
from tkinter import messagebox
from PIL import Image, ImageTk   # pip install pillow

FRAME_INTERVAL_MS = 40  # ~25 fps


class OverlayClientGUI:
    def __init__(self, master, cache_dir, start_seq=1):
        """
        cache_dir: diretório onde estão os ficheiros cache-<seq>.jpg
        (ex: cache/O3/videoA)
        """
        self.master = master
        self.master.title(f"Overlay OTT Client - {cache_dir}")

        self.cache_dir = cache_dir
        self.seq = start_seq
        self.running = False
        self.photo = None

        self._build_widgets()
        self.master.protocol("WM_DELETE_WINDOW", self.quit)

    def _build_widgets(self):
        # zona de botões (Play / Pause / Quit)
        buttons = tk.Frame(self.master)
        buttons.pack(side=tk.TOP, padx=5, pady=5)

        self.play_btn = tk.Button(buttons, width=10, padx=3, pady=3,
                                  text="Play", command=self.play)
        self.play_btn.pack(side=tk.LEFT, padx=2)

        self.pause_btn = tk.Button(buttons, width=10, padx=3, pady=3,
                                   text="Pause", command=self.pause)
        self.pause_btn.pack(side=tk.LEFT, padx=2)

        self.quit_btn = tk.Button(buttons, width=10, padx=3, pady=3,
                                  text="Quit", command=self.quit)
        self.quit_btn.pack(side=tk.LEFT, padx=2)

        # label onde o vídeo é desenhado
        self.image_label = tk.Label(self.master, bd=0)
        self.image_label.pack(side=tk.BOTTOM, padx=5, pady=5)

    # -------------------------
    # Controlo
    # -------------------------
    def play(self):
        if not self.running:
            self.running = True
            self.update_frame()

    def pause(self):
        self.running = False

    def quit(self):
        self.running = False
        self.master.destroy()

    # -------------------------
    # Atualização de frames
    # -------------------------
    def update_frame(self):
        if not self.running:
            return

        filename = os.path.join(self.cache_dir, f"cache-{self.seq}.jpg")

        if os.path.exists(filename):
            try:
                img = Image.open(filename)
                self.photo = ImageTk.PhotoImage(img)
                self.image_label.config(image=self.photo)
                self.seq += 1
            except Exception as e:
                print(f"[GUI] Erro a carregar {filename}: {e}")
        else:
            # frame ainda não existe, esperar por nova
            pass

        self.master.after(FRAME_INTERVAL_MS, self.update_frame)


def main():
    parser = argparse.ArgumentParser(description="Overlay OTT Client GUI")
    parser.add_argument("--client", required=True, help="nome do nó cliente (ex: O3)")
    parser.add_argument("--stream", required=True, help="id da stream (ex: videoA)")
    parser.add_argument("--start-seq", type=int, default=1, help="número da primeira frame (default: 1)")
    args = parser.parse_args()

    # diretório cache/<client>/<stream>/
    cache_dir = os.path.join("cache", args.client, args.stream)
    if not os.path.isdir(cache_dir):
        print(f"Diretório {cache_dir} não existe (ainda). Certifica-te que o cliente já está a receber frames.")
        # não saímos à força; a GUI pode esperar as primeiras frames

    root = tk.Tk()
    gui = OverlayClientGUI(root, cache_dir=cache_dir, start_seq=args.start_seq)
    root.mainloop()


if __name__ == "__main__":
    main()
