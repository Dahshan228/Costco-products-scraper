import tkinter as tk
import ttkbootstrap as ttk
from ttkbootstrap.constants import *
from tkinter import scrolledtext, messagebox
import sys
import threading
import costco_scraper
import os
import subprocess

class TextRedirector(object):
    def __init__(self, widget, tag="stdout"):
        self.widget = widget
        self.tag = tag

    def write(self, str):
        self.widget.configure(state="normal")
        self.widget.insert("end", str, (self.tag,))
        self.widget.see("end")
        self.widget.configure(state="disabled")

    def flush(self):
        pass

class CostcoScraperGUI:
    def __init__(self, root):
        self.root = root
        self.root.title("Costco Warehouse Scraper")
        self.root.geometry("700x800")
        
        # Data
        self.warehouses = costco_scraper.get_warehouses()
        self.filtered_warehouses = self.warehouses.copy()

        # UI Components
        self.create_widgets()
        
        # Redirect stdout
        sys.stdout = TextRedirector(self.log_area, "stdout")
        sys.stderr = TextRedirector(self.log_area, "stderr")
        
        print(f"Loaded {len(self.warehouses)} warehouses.")

    def create_widgets(self):
        # Main Container
        main_frame = ttk.Frame(self.root, padding=20)
        main_frame.pack(fill="both", expand=True)

        # 1. Title
        title_label = ttk.Label(
            main_frame, 
            text="Costco Warehouse Scraper", 
            bootstyle="primary",
            font=("Helvetica", 20, "bold")
        )
        title_label.pack(pady=(0, 20))

        # 2. Search Frame
        search_frame = ttk.Labelframe(main_frame, text="Filter Locations", padding=15)
        search_frame.pack(fill="x", pady=(0, 10))

        ttk.Label(search_frame, text="Search:").pack(side="left")
        self.search_var = ttk.StringVar()
        self.search_var.trace_add("write", self.update_list)
        self.search_entry = ttk.Entry(search_frame, textvariable=self.search_var)
        self.search_entry.pack(side="left", fill="x", expand=True, padx=10)

        # 3. Listbox with Scrollbar (Multi-Select)
        list_frame = ttk.Labelframe(main_frame, text="Select Warehouses (Multi-select enabled, ctl + leftclick)", padding=10)
        list_frame.pack(fill="both", expand=True, pady=(0, 10))

        # Using Tkinter Listbox because ttkbootstrap table is overkill/complex for simple multi-select
        # We can style it manually or wrap it
        self.listbox = tk.Listbox(
            list_frame, 
            height=15, 
            selectmode="extended", 
            font=("Consolas", 11),
            activestyle="none",
            highlightthickness=0,
            bd=1,
            relief="solid"
        )
        scrollbar = ttk.Scrollbar(list_frame, orient="vertical", command=self.listbox.yview)
        self.listbox.configure(yscrollcommand=scrollbar.set)

        self.listbox.pack(side="left", fill="both", expand=True)
        scrollbar.pack(side="right", fill="y")

        self.populate_list()

        # 4. Buttons
        btn_frame = ttk.Frame(main_frame)
        btn_frame.pack(fill="x", pady=(0, 20))

        self.scrape_btn = ttk.Button(
            btn_frame, 
            text="Scrape Selected Warehouses", 
            command=self.start_scrape_thread,
            bootstyle="success-outline",
            width=25
        )
        self.scrape_btn.pack(side="left", fill="x", expand=True, padx=(0, 10))
        
        exit_btn = ttk.Button(
            btn_frame, 
            text="Exit", 
            command=self.root.quit, 
            bootstyle="danger-outline"
        )
        exit_btn.pack(side="right")

        # 5. Log Area
        log_frame = ttk.Labelframe(main_frame, text="Live Log Output", padding=10)
        log_frame.pack(fill="both", expand=True)

        self.log_area = scrolledtext.ScrolledText(log_frame, state='disabled', height=10, font=("Consolas", 10))
        self.log_area.pack(fill="both", expand=True)
        self.log_area.tag_config("stdout", foreground="#2c3e50") # Dark gray
        self.log_area.tag_config("stderr", foreground="#e74c3c") # Red

    def populate_list(self):
        self.listbox.delete(0, tk.END)
        for w in self.filtered_warehouses:
            display_text = f"{w['name']} ({w['state']}) - ID: {w['id']}"
            self.listbox.insert(tk.END, display_text)

    def update_list(self, *args):
        search_term = self.search_var.get().lower()
        self.filtered_warehouses = [
            w for w in self.warehouses 
            if search_term in w['name'].lower() or search_term in w['id']
        ]
        self.populate_list()

    def start_scrape_thread(self):
        selections = self.listbox.curselection()
        if not selections:
            messagebox.showwarning("No Selection", "Please select at least one warehouse.")
            return

        selected_warehouses = [self.filtered_warehouses[i] for i in selections]
        
        self.scrape_btn.config(state="disabled", text="Scraping in progress...")
        
        thread = threading.Thread(target=self.run_batch_scrape, args=(selected_warehouses,))
        thread.daemon = True
        thread.start()

    def run_batch_scrape(self, warehouses):
        try:
            generated_files = []
            for i, warehouse in enumerate(warehouses):
                print(f"\n--- Batch {i+1}/{len(warehouses)}: {warehouse['name']} ---")
                # We need to capture the filename. 
                # Ideally, scrape_warehouse should return the filename, but we can infer it or modify scraper.
                # For now, let's just run it. The user sees the log.
                
                # To get the filename, we can replicate the naming logic:
                safe_name = "".join([c if c.isalnum() else "_" for c in warehouse['name']])
                filename = f"costco_scrape_{warehouse['id']}_{safe_name}_products.csv"
                generated_files.append(filename)
                
                costco_scraper.scrape_warehouse(warehouse)
            
            messagebox.showinfo("Batch Complete", f"Successfully scraped {len(warehouses)} warehouses.")
            
            # Auto-open the Last Created CSV or the Folder?
            # Let's open the first one to show proof, or open the folder.
            # Opening folder is safer since there are multiple.
            subprocess.run(["open", "."]) 

        except Exception as e:
            print(f"Error during scraping: {e}", file=sys.stderr)
            messagebox.showerror("Error", f"An error occurred:\n{e}")
        finally:
            self.root.after(0, lambda: self.scrape_btn.config(state="normal", text="Scrape Selected Warehouses"))

if __name__ == "__main__":
    # Theme Setup
    root = ttk.Window(themename="cosmo") # Modern, light/clean theme
    app = CostcoScraperGUI(root)
    root.mainloop()
