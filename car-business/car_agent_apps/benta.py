import os
import json
import shutil
import tkinter as tk
from tkinter import messagebox, ttk

# Paths to your scripts
COPY_SCRIPT_PATH = "D:/carsfsale/1/copy.py"
REPHRASE_SCRIPT_PATH = "D:/carsfsale/1/rephrase.py"
SHORT_CAPTION_SCRIPT_PATH = "D:/carsfsale/1/short_caption.py"
COPY_SHORT_SCRIPT_PATH = "D:/carsfsale/1/copy_short.py"
DETAILS_FILENAME = "details.txt"
HISTORY_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "seller_history.json")

def load_seller_history():
    if os.path.isfile(HISTORY_FILE):
        with open(HISTORY_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return []

def save_seller_history(names):
    with open(HISTORY_FILE, "w", encoding="utf-8") as f:
        json.dump(names, f, ensure_ascii=False, indent=2)

def create_folder():
    car_type = type_var.get()
    car_name = name_entry.get().strip()
    car_model = model_entry.get().strip()
    car_price = price_entry.get().strip()
    seller_name = seller_combobox.get().strip()

    if not all([car_name, car_model, car_price]):
        messagebox.showerror("Error", "Please fill in all fields.")
        return

    folder_name = f"{car_type} {car_name} {car_model} {car_price}"
    try:
        os.makedirs(folder_name, exist_ok=False)
    except FileExistsError:
        messagebox.showerror("Error", f"The folder '{folder_name}' already exists.")
        return

    with open(os.path.join(folder_name, DETAILS_FILENAME), "w") as file:
        file.write(f"{car_model} {car_name}\n")

    if seller_name:
        with open(os.path.join(folder_name, f"{seller_name}.txt"), "w") as file:
            file.write(seller_name)

    # Copy scripts
    for script_path in (COPY_SCRIPT_PATH, REPHRASE_SCRIPT_PATH,
                        SHORT_CAPTION_SCRIPT_PATH, COPY_SHORT_SCRIPT_PATH):
        if os.path.isfile(script_path):
            shutil.copy(script_path, os.path.join(folder_name, os.path.basename(script_path)))
        else:
            messagebox.showwarning("Warning", f"Could not find: {script_path}")

    # Save seller to history
    if seller_name:
        history = load_seller_history()
        if seller_name not in history:
            history.append(seller_name)
            save_seller_history(history)

    messagebox.showinfo("Success", f"Folder '{folder_name}' and details file created.")
    os.startfile(folder_name)

    # Clear fields
    name_entry.delete(0, tk.END)
    model_entry.delete(0, tk.END)
    price_entry.delete(0, tk.END)
    seller_combobox.set("")

root = tk.Tk()
root.title("Car Folder Creator")

tk.Label(root, text="Type:").grid(row=0, column=0, padx=5, pady=5)
tk.Label(root, text="Name:").grid(row=1, column=0, padx=5, pady=5)
tk.Label(root, text="Model:").grid(row=2, column=0, padx=5, pady=5)
tk.Label(root, text="Price:").grid(row=3, column=0, padx=5, pady=5)
tk.Label(root, text="Seller:").grid(row=4, column=0, padx=5, pady=5)

type_var = tk.StringVar(value="Cash")
type_option = tk.OptionMenu(root, type_var, "Cash", "Finance", "Pasalo")
type_option.grid(row=0, column=1, padx=5, pady=5)

name_entry = tk.Entry(root)
model_entry = tk.Entry(root)
price_entry = tk.Entry(root)
name_entry.grid(row=1, column=1, padx=5, pady=5)
model_entry.grid(row=2, column=1, padx=5, pady=5)
price_entry.grid(row=3, column=1, padx=5, pady=5)

seller_history = load_seller_history()
seller_combobox = ttk.Combobox(root, values=seller_history)
seller_combobox.grid(row=4, column=1, padx=5, pady=5)

create_button = tk.Button(root, text="Create Folder", command=create_folder)
create_button.grid(row=5, column=0, columnspan=2, pady=10)

root.mainloop()
