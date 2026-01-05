import tkinter as tk
from tkinter import ttk, scrolledtext, messagebox
import sys
import threading
import costco_scraper

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
        self.root.geometry("600x700")

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
        # 1. Title
        title_label = ttk.Label(self.root, text="Costco Warehouse Scraper", font=("Helvetica", 16, "bold"))
        title_label.pack(pady=10)

        # 2. Search Frame
        search_frame = ttk.Frame(self.root)
        search_frame.pack(fill="x", padx=10, pady=5)

        ttk.Label(search_frame, text="Search Warehouse:").pack(side="left")
        self.search_var = tk.StringVar()
        self.search_var.trace_add("write", self.update_list)
        self.search_entry = ttk.Entry(search_frame, textvariable=self.search_var)
        self.search_entry.pack(side="left", fill="x", expand=True, padx=5)

        # 3. Listbox with Scrollbar
        list_frame = ttk.Frame(self.root)
        list_frame.pack(fill="both", expand=True, padx=10, pady=5)

        self.listbox = tk.Listbox(list_frame, height=15, selectmode="single", font=("Courier", 10))
        scrollbar = ttk.Scrollbar(list_frame, orient="vertical", command=self.listbox.yview)
        self.listbox.configure(yscrollcommand=scrollbar.set)

        self.listbox.pack(side="left", fill="both", expand=True)
        scrollbar.pack(side="right", fill="y")

        self.populate_list()

        # 4. Buttons
        btn_frame = ttk.Frame(self.root)
        btn_frame.pack(fill="x", padx=10, pady=10)

        self.scrape_btn = ttk.Button(btn_frame, text="Scrape Selected Warehouse", command=self.start_scrape_thread)
        self.scrape_btn.pack(side="left", fill="x", expand=True, padx=5)
        
        exit_btn = ttk.Button(btn_frame, text="Exit", command=self.root.quit)
        exit_btn.pack(side="right", padx=5)

        # 5. Log Area
        log_frame = ttk.LabelFrame(self.root, text="Logs & Output")
        log_frame.pack(fill="both", expand=True, padx=10, pady=5)

        self.log_area = scrolledtext.ScrolledText(log_frame, state='disabled', height=10, font=("Consolas", 9))
        self.log_area.pack(fill="both", expand=True, padx=5, pady=5)
        self.log_area.tag_config("stdout", foreground="black")
        self.log_area.tag_config("stderr", foreground="red")

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
        selection = self.listbox.curselection()
        if not selection:
            messagebox.showwarning("No Selection", "Please select a warehouse from the list.")
            return

        index = selection[0]
        warehouse = self.filtered_warehouses[index]
        
        self.scrape_btn.config(state="disabled")
        thread = threading.Thread(target=self.run_scrape, args=(warehouse,))
        thread.daemon = True
        thread.start()

    def run_scrape(self, warehouse):
        try:
            costco_scraper.scrape_warehouse(warehouse)
            messagebox.showinfo("Success", f"Scraping completed for {warehouse['name']}")
        except Exception as e:
            print(f"Error during scraping: {e}", file=sys.stderr)
            messagebox.showerror("Error", f"An error occurred:\n{e}")
        finally:
            self.root.after(0, lambda: self.scrape_btn.config(state="normal"))

if __name__ == "__main__":
    root = tk.Tk()
    app = CostcoScraperGUI(root)
    root.mainloop()
