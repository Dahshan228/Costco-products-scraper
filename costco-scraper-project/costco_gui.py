import os
import subprocess
import sys
import threading
import tkinter as tk
from pathlib import Path
from tkinter import filedialog, messagebox, scrolledtext

import ttkbootstrap as ttk

import costco_scraper


class TextRedirector:
    def __init__(self, widget, tag="stdout"):
        self.widget = widget
        self.tag = tag

    def write(self, value):
        self.widget.configure(state="normal")
        self.widget.insert("end", value, (self.tag,))
        self.widget.see("end")
        self.widget.configure(state="disabled")

    def flush(self):
        return None


class CostcoScraperGUI:
    def __init__(self, root):
        self.root = root
        self.root.title("Costco Warehouse Scraper")
        self.root.geometry("780x860")

        self.warehouses = costco_scraper.get_warehouses()
        self.filtered_warehouses = self.warehouses.copy()

        self.default_config = costco_scraper.ScraperConfig.from_env()

        self.create_widgets()

        sys.stdout = TextRedirector(self.log_area, "stdout")
        sys.stderr = TextRedirector(self.log_area, "stderr")

        print(f"Loaded {len(self.warehouses)} warehouses.")

    def create_widgets(self):
        main_frame = ttk.Frame(self.root, padding=20)
        main_frame.pack(fill="both", expand=True)

        title_label = ttk.Label(
            main_frame,
            text="Costco Warehouse Scraper",
            bootstyle="primary",
            font=("Helvetica", 20, "bold"),
        )
        title_label.pack(pady=(0, 20))

        search_frame = ttk.Labelframe(main_frame, text="Filter Locations", padding=15)
        search_frame.pack(fill="x", pady=(0, 10))

        ttk.Label(search_frame, text="Search:").pack(side="left")
        self.search_var = ttk.StringVar()
        self.search_var.trace_add("write", self.update_list)
        self.search_entry = ttk.Entry(search_frame, textvariable=self.search_var)
        self.search_entry.pack(side="left", fill="x", expand=True, padx=10)

        output_frame = ttk.Labelframe(main_frame, text="Output", padding=15)
        output_frame.pack(fill="x", pady=(0, 10))

        ttk.Label(output_frame, text="Output Folder:").pack(side="left")
        self.output_dir_var = ttk.StringVar(value=str(self.default_config.output_dir))
        self.output_dir_entry = ttk.Entry(output_frame, textvariable=self.output_dir_var)
        self.output_dir_entry.pack(side="left", fill="x", expand=True, padx=10)
        ttk.Button(output_frame, text="Browse", command=self.choose_output_dir, bootstyle="secondary-outline").pack(side="left")

        self.open_folder_var = ttk.BooleanVar(value=True)
        ttk.Checkbutton(output_frame, text="Open folder when done", variable=self.open_folder_var, bootstyle="round-toggle").pack(
            side="left", padx=(10, 0)
        )

        list_frame = ttk.Labelframe(main_frame, text="Select Warehouses (Multi-select enabled)", padding=10)
        list_frame.pack(fill="both", expand=True, pady=(0, 10))

        self.listbox = tk.Listbox(
            list_frame,
            height=15,
            selectmode="extended",
            font=("Consolas", 11),
            activestyle="none",
            highlightthickness=0,
            bd=1,
            relief="solid",
        )
        scrollbar = ttk.Scrollbar(list_frame, orient="vertical", command=self.listbox.yview)
        self.listbox.configure(yscrollcommand=scrollbar.set)

        self.listbox.pack(side="left", fill="both", expand=True)
        scrollbar.pack(side="right", fill="y")

        self.populate_list()

        btn_frame = ttk.Frame(main_frame)
        btn_frame.pack(fill="x", pady=(0, 20))

        self.scrape_btn = ttk.Button(
            btn_frame,
            text="Scrape Selected Warehouses",
            command=self.start_scrape_thread,
            bootstyle="success-outline",
            width=25,
        )
        self.scrape_btn.pack(side="left", fill="x", expand=True, padx=(0, 10))

        exit_btn = ttk.Button(btn_frame, text="Exit", command=self.root.quit, bootstyle="danger-outline")
        exit_btn.pack(side="right")

        log_frame = ttk.Labelframe(main_frame, text="Live Log Output", padding=10)
        log_frame.pack(fill="both", expand=True)

        self.log_area = scrolledtext.ScrolledText(log_frame, state="disabled", height=10, font=("Consolas", 10))
        self.log_area.pack(fill="both", expand=True)
        self.log_area.tag_config("stdout", foreground="#2c3e50")
        self.log_area.tag_config("stderr", foreground="#e74c3c")

    def choose_output_dir(self):
        chosen = filedialog.askdirectory(initialdir=self.output_dir_var.get() or str(Path.cwd()))
        if chosen:
            self.output_dir_var.set(chosen)

    def populate_list(self):
        self.listbox.delete(0, tk.END)
        for warehouse in self.filtered_warehouses:
            display_text = f"{warehouse['name']} ({warehouse['state']}) - ID: {warehouse['id']}"
            self.listbox.insert(tk.END, display_text)

    def update_list(self, *args):
        search_term = self.search_var.get().lower()
        self.filtered_warehouses = [
            warehouse
            for warehouse in self.warehouses
            if search_term in warehouse["name"].lower() or search_term in warehouse["id"]
        ]
        self.populate_list()

    def start_scrape_thread(self):
        selections = self.listbox.curselection()
        if not selections:
            messagebox.showwarning("No Selection", "Please select at least one warehouse.")
            return

        selected_warehouses = [self.filtered_warehouses[index] for index in selections]

        output_dir = self.output_dir_var.get().strip()
        if not output_dir:
            messagebox.showwarning("Missing Output Folder", "Please select an output folder.")
            return

        config = costco_scraper.ScraperConfig.from_env(output_dir=output_dir)

        self.scrape_btn.config(state="disabled", text="Scraping in progress...")
        thread = threading.Thread(
            target=self.run_batch_scrape,
            args=(selected_warehouses, config, self.open_folder_var.get()),
            daemon=True,
        )
        thread.start()

    def run_batch_scrape(self, warehouses, config, open_when_done):
        successes = []
        failures = []

        try:
            for index, warehouse in enumerate(warehouses, start=1):
                print(f"\n--- Batch {index}/{len(warehouses)}: {warehouse['name']} ---")
                result = costco_scraper.scrape_warehouse(warehouse, config=config)

                if result.get("success"):
                    successes.append(result)
                    print(f"Saved: {result['output_file']} ({result['rows']} rows)")
                    failed_graphql = result.get("failed_graphql_batches")
                    if failed_graphql:
                        print(f"Warning: {failed_graphql} GraphQL enrichment batches failed.", file=sys.stderr)
                else:
                    failures.append(result)
                    print(f"Failed: {warehouse['name']} - {result.get('error')}", file=sys.stderr)

            if failures and successes:
                messagebox.showwarning(
                    "Batch Complete (Partial)",
                    f"Completed with partial success.\nSucceeded: {len(successes)}\nFailed: {len(failures)}",
                )
            elif failures and not successes:
                messagebox.showerror("Batch Failed", f"All selected warehouses failed ({len(failures)} total).")
            else:
                messagebox.showinfo("Batch Complete", f"Successfully scraped {len(successes)} warehouses.")

            if open_when_done and successes:
                self.open_folder(config.output_dir)

        except Exception as exc:
            print(f"Error during scraping: {exc}", file=sys.stderr)
            messagebox.showerror("Error", f"An unexpected error occurred:\n{exc}")
        finally:
            self.root.after(0, lambda: self.scrape_btn.config(state="normal", text="Scrape Selected Warehouses"))

    @staticmethod
    def open_folder(path: Path):
        path_str = str(path)
        try:
            if sys.platform.startswith("darwin"):
                subprocess.run(["open", path_str], check=False)
            elif os.name == "nt":
                os.startfile(path_str)  # type: ignore[attr-defined]
            else:
                subprocess.run(["xdg-open", path_str], check=False)
        except Exception as exc:
            print(f"Could not open output folder automatically: {exc}", file=sys.stderr)


def main():
    root = ttk.Window(themename="cosmo")
    CostcoScraperGUI(root)
    root.mainloop()


if __name__ == "__main__":
    main()
