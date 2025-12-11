# OverlayClientGUI.py
# GUI simples baseada no ClientGUI do Kurose,
# mas em vez de falar com a rede, lê ficheiros cache-<seq>.jpg
# que são criados pelo Node em modo --client.

import os
import tkinter as tk
from tkinter import messagebox
from PIL import Image, ImageTk  # precisa de "pip install pillow"

FRAME_INTERVAL_MS = 40  # ~25 fps (ajusta conforme a taxa de envio do servidor)


class OverlayClientGUI:
    def __init__(self, master, cache_prefix="cache-", start_seq=1):
        self.master = master
        self.master.title("Overlay Client Video")

        self.cache_prefix = cache_prefix
        self.seq = start_seq
        self.running = False
        self.image_label = None
        self.photo = None

        self._build_widgets()

    def _build_widgets(self):
        # Botões semelhantes ao ClientGUI original
        buttons_frame = tk.Frame(self.master)
        buttons_frame.pack(side=tk.TOP, padx=5, pady=5)

        self.play_button = tk.Button(buttons_frame, text="Play", command=self.play)
        self.play_button.pack(side=tk.LEFT, padx=2)

        self.pause_button = tk.Button(buttons_frame, text="Pause", command=self.pause)
        self.pause_button.pack(side=tk.LEFT, padx=2)

        self.quit_button = tk.Button(buttons_frame, text="Quit", command=self.quit)
        self.quit_button.pack(side=tk.LEFT, padx=2)

        # Área de vídeo (label onde a imagem é colocada)
        self.image_label = tk.Label(self.master)
        self.image_label.pack(side=tk.BOTTOM, padx=5, pady=5)

        # Ao fechar a janela
        self.master.protocol("WM_DELETE_WINDOW", self.quit)

    def play(self):
        if not self.running:
            self.running = True
            self.update_frame()

    def pause(self):
        self.running = False

    def quit(self):
        self.running = False
        self.master.destroy()

    def update_frame(self):
        if not self.running:
            return

        filename = f"{self.cache_prefix}{self.seq}.jpg"
        if os.path.exists(filename):
            try:
                img = Image.open(filename)
                self.photo = ImageTk.PhotoImage(img)
                self.image_label.config(image=self.photo)
                self.seq += 1
            except Exception as e:
                print(f"Erro a carregar {filename}: {e}")
        else:
            # Se a frame ainda não existe, espera um pouco e volta a tentar
            # Isto permite sincronizar com o Node que ainda está a gravar frames.
            pass

        # agenda próxima atualização
        self.master.after(FRAME_INTERVAL_MS, self.update_frame)


if __name__ == "__main__":
    root = tk.Tk()
    app = OverlayClientGUI(root, cache_prefix="cache-", start_seq=1)
    root.mainloop()
