"""
Messenger Car Scraper v3 — GUI
-------------------------------
Navigate to any Facebook chat in the browser.
Extracts text + images from the center message feed only (no sidebar).
Simple tkinter GUI with buttons instead of terminal prompts.
"""
import os, re, sys, time, json, base64, hashlib, shutil, threading, queue, traceback
from pathlib import Path
from datetime import datetime
import tkinter as tk
from tkinter import scrolledtext, ttk, messagebox

from dotenv import load_dotenv

load_dotenv()

import messenger_scraper_v3 as scraper


class _LogRedirect:
    def __init__(self):
        self.queue = None

    def __call__(self, msg):
        if self.queue is not None:
            self.queue.put(("log", msg))

_log_redirect = _LogRedirect()
scraper.log = _log_redirect


class MessengerScraperGUI:
    def __init__(self):
        self.root = tk.Tk()
        self.root.title("Messenger Car Scraper v3")
        self.root.geometry("980x760")

        self._queue = queue.Queue()
        _log_redirect.queue = self._queue

        self._input_event = threading.Event()
        self._input_result = None

        self._worker_event = threading.Event()
        self._worker_task = None
        self._worker_thread = None

        self._browser_ready = False
        self._running = False
        self._batch_num = 0
        self._total_msgs = 0
        self._messages = []

        self._build_gui()
        self._poll_queue()

    # ── GUI building ────────────────────────────────────────

    def _build_gui(self):
        msg_frame = ttk.LabelFrame(self.root, text="Messages (current batch)")
        msg_frame.pack(fill="both", expand=True, padx=8, pady=(8, 2))

        tree_frame = ttk.Frame(msg_frame)
        tree_frame.pack(fill="both", expand=True)

        self._tree = ttk.Treeview(
            tree_frame,
            columns=("idx", "type", "preview", "car"),
            show="headings",
            height=7,
        )
        vsb = ttk.Scrollbar(tree_frame, orient="vertical", command=self._tree.yview)
        self._tree.configure(yscrollcommand=vsb.set)

        self._tree.heading("idx", text="#")
        self._tree.heading("type", text="Type")
        self._tree.heading("preview", text="Preview")
        self._tree.heading("car", text="Car?")

        self._tree.column("idx", width=40, anchor="center")
        self._tree.column("type", width=90, anchor="center")
        self._tree.column("preview", width=680)
        self._tree.column("car", width=50, anchor="center")

        self._tree.pack(side="left", fill="both", expand=True)
        vsb.pack(side="right", fill="y")

        self._tree.tag_configure("car", background="#e8f5e9")

        # Log
        log_frame = ttk.LabelFrame(self.root, text="Log")
        log_frame.pack(fill="both", expand=True, padx=8, pady=2)

        self._log_widget = scrolledtext.ScrolledText(
            log_frame, height=10, state="disabled",
            font=("Consolas", 9), wrap="word",
            bg="#1e1e1e", fg="#d4d4d4", insertbackground="white",
        )
        self._log_widget.pack(fill="both", expand=True, padx=2, pady=2)

        # Prompt
        self._prompt_var = tk.StringVar(value="")
        prompt_lbl = ttk.Label(self.root, textvariable=self._prompt_var, foreground="#0066cc")
        prompt_lbl.pack(anchor="w", padx=10, pady=(4, 0))

        # Contextual buttons
        self._btn_frame = ttk.Frame(self.root)
        self._btn_frame.pack(fill="x", padx=8, pady=2)

        self._btn_after = ttk.Button(self._btn_frame, text="After", command=lambda: self._input_answer("a"))
        self._btn_before = ttk.Button(self._btn_frame, text="Before", command=lambda: self._input_answer("b"))
        self._btn_skip = ttk.Button(self._btn_frame, text="Skip", command=lambda: self._input_answer("s"))
        self._btn_done = ttk.Button(self._btn_frame, text="Done", command=lambda: self._input_answer("d"))

        ttk.Separator(self._btn_frame, orient="vertical").pack(side="left", fill="y", padx=4)

        self._btn_again = ttk.Button(self._btn_frame, text="Scrape Again", command=self._start_scrape)
        self._btn_retry = ttk.Button(self._btn_frame, text="Retry", command=self._start_scrape)
        self._btn_debug = ttk.Button(self._btn_frame, text="Debug", command=self._debug_current)

        self._hide_car_btns()
        self._hide_batch_btns()

        # Bottom controls
        ctrl = ttk.Frame(self.root)
        ctrl.pack(fill="x", padx=8, pady=(2, 6))

        self._start_btn = ttk.Button(ctrl, text="\u25b6 Start Browser", command=self._start_browser)
        self._start_btn.pack(side="left", padx=2)

        self._scrape_btn = ttk.Button(ctrl, text="\u25b6\u25b6 Scrape Batch", command=self._start_scrape)
        self._scrape_btn.configure(state="disabled")
        self._scrape_btn.pack(side="left", padx=2)

        self._debug_btn = ttk.Button(ctrl, text="Debug DOM", command=self._debug_current)
        self._debug_btn.pack(side="left", padx=2)

        ttk.Button(ctrl, text="\u2715 Exit", command=self._on_exit).pack(side="right", padx=2)

        # Status bar
        self._status_var = tk.StringVar(value="Click \u2018Start Browser\u2019 to launch Chrome")
        status = ttk.Label(self.root, textvariable=self._status_var, relief="sunken", anchor="w")
        status.pack(fill="x", padx=8, pady=(0, 6))

    def _hide_car_btns(self):
        for b in (self._btn_after, self._btn_before, self._btn_skip, self._btn_done):
            b.pack_forget()

    def _hide_batch_btns(self):
        for b in (self._btn_again, self._btn_retry, self._btn_debug):
            b.pack_forget()

    def _show_car_btns(self):
        self._btn_after.pack(side="left", padx=2)
        self._btn_before.pack(side="left", padx=2)
        self._btn_skip.pack(side="left", padx=2)
        self._btn_done.pack(side="left", padx=2)

    def _show_batch_btns(self):
        self._btn_again.pack(side="left", padx=2)
        self._btn_retry.pack(side="left", padx=2)
        self._btn_debug.pack(side="left", padx=2)

    def _log(self, msg):
        self._log_widget.configure(state="normal")
        now = datetime.now().strftime("%H:%M:%S")
        self._log_widget.insert("end", f"[{now}] {msg}\n")
        self._log_widget.see("end")
        self._log_widget.configure(state="disabled")

    # ── Queue polling ───────────────────────────────────────

    def _poll_queue(self):
        try:
            while True:
                item = self._queue.get_nowait()
                kind = item[0]

                if kind == "log":
                    self._log(item[1])
                elif kind == "set_messages":
                    self._set_messages(item[1])
                elif kind == "prompt_car":
                    self._prompt_car(item[1], item[2], item[3])
                elif kind == "prompt_batch":
                    self._prompt_batch(item[1] if len(item) > 1 else "")
                elif kind == "status":
                    self._status_var.set(item[1])
                elif kind == "browser_ready":
                    self._browser_ready = True
                    self._start_btn.configure(text="\u2713 Browser Running", state="disabled")
                    self._scrape_btn.configure(state="normal")
                    self._status_var.set("Browser open. Navigate to a Facebook chat, then click Scrape Batch")
                elif kind == "done":
                    self._running = False
                    self._scrape_btn.configure(state="normal" if self._browser_ready else "disabled")
                    self._status_var.set("Ready")
                elif kind == "error":
                    messagebox.showerror("Error", item[1])
                    self._log(f"[X] {item[1]}")
        except queue.Empty:
            pass
        self.root.after(100, self._poll_queue)

    def _set_messages(self, messages):
        self._tree.delete(*self._tree.get_children())
        for m in messages:
            idx = m["index"]
            mtype = m["type"].upper()
            preview = m["text"][:120] if m["text"] else f"{len(m.get('image_urls', []))} images"
            is_car = "\u2713" if mtype == "TEXT" and scraper.is_car_related(m["text"]) else ""
            tags = ("car",) if is_car else ()
            self._tree.insert("", "end", values=(idx, mtype, preview, is_car), tags=tags)

    def _prompt_car(self, num, total, text_preview):
        self._prompt_var.set(f"Car {num}/{total}: {text_preview[:200]}")
        self._show_car_btns()
        self._hide_batch_btns()
        self._status_var.set(f"Waiting for input \u2014 Car {num}/{total}")

    def _prompt_batch(self, msg):
        self._prompt_var.set(msg or "Batch complete \u2014 what next?")
        self._hide_car_btns()
        self._show_batch_btns()
        self._status_var.set(msg or "Batch complete")

    def _input_answer(self, value):
        self._hide_car_btns()
        self._hide_batch_btns()
        self._prompt_var.set("")
        self._input_result = value
        self._input_event.set()

    # ── Worker thread (owns all Playwright objects) ─────────

    def _signal_worker(self, task):
        """Send a task to the persistent worker thread."""
        self._worker_task = task
        self._worker_event.set()

    def _start_browser(self):
        if self._browser_ready or self._running:
            return
        self._start_btn.configure(state="disabled")
        self._status_var.set("Launching Chrome...")

        self._worker_thread = threading.Thread(target=self._worker_main, daemon=True)
        self._worker_thread.start()

    def _worker_main(self):
        """Single persistent thread that owns all Playwright objects."""
        _log_redirect.queue = self._queue

        playwright = None
        context = None
        page = None

        try:
            # ── Launch browser ──
            scraper.ensure_dir(scraper.OUTPUT_DIR)
            scraper.ensure_dir(scraper.TEST_OUTPUT_DIR)
            Path(scraper.PROFILE_DIR).mkdir(parents=True, exist_ok=True)

            self._queue.put(("log", "Launching Chrome (first time, one-time login)..."))

            playwright = __import__("playwright").sync_api.sync_playwright().start()
            context = playwright.chromium.launch_persistent_context(
                user_data_dir=scraper.PROFILE_DIR,
                headless=False,
                channel="chrome",
                args=["--no-first-run", "--no-default-browser-check", "--disable-session-crashed-bubble"],
            )
            page = context.pages[0] if context.pages else context.new_page()

            self._queue.put(("browser_ready",))
            self._queue.put(("log", "Chrome is ready. Navigate to any Facebook chat, then click Scrape Batch"))
            self._queue.put(("log", "If you see a login page, log into Facebook now (one-time)."))

            # ── Task loop ──
            while True:
                self._worker_event.wait()
                self._worker_event.clear()
                task = self._worker_task
                self._worker_task = None

                if task == "exit":
                    break
                elif task == "scrape":
                    self._do_scrape(page, context)
                elif task == "debug":
                    self._do_debug(page)

        except Exception as e:
            self._queue.put(("error", str(e)))
            self._queue.put(("log", traceback.format_exc()))
        finally:
            try:
                if context:
                    context.close()
                if playwright:
                    playwright.stop()
            except Exception:
                pass
            self._queue.put(("status", "Browser closed"))

    # ── Scrape logic (runs inside worker thread) ────────────

    def _do_scrape(self, page, context):
        self._queue.put(("set_messages", []))
        self._queue.put(("log", f"{'='*20} Batch {self._batch_num + 1} {'='*20}"))
        time.sleep(1)

        try:
            result = scraper.extract_messages(page)

            if isinstance(result, dict) and "error" in result:
                self._queue.put(("log", f"[X] {result['error']}"))
                self._queue.put(("prompt_batch", "No messages extracted"))
                return

            messages = [m for m in result if "_feed" not in m]
            feed = next((m for m in result if "_feed" in m), None)

            if feed:
                self._queue.put(("log", f"Feed: {feed['_feed']}  (method: {feed['_method']})"))
            self._queue.put(("log", f"Messages: {len(messages)}"))
            self._queue.put(("set_messages", messages))

            self._messages = messages

            car_idx = [(i, m) for i, m in enumerate(messages)
                       if m["type"] == "text" and scraper.is_car_related(m["text"])]

            if not car_idx:
                self._queue.put(("log", "No car-related messages found."))
                self._queue.put(("prompt_batch", "No car listings detected"))
                return

            self._queue.put(("log", f"Found {len(car_idx)} car-related text(s)"))

            answers = []
            for g_idx, (msg_idx, tm) in enumerate(car_idx):
                self._input_event.clear()
                self._queue.put(("prompt_car", g_idx + 1, len(car_idx), tm["text"][:200]))
                self._input_event.wait()

                ans = self._input_result
                if ans == "d":
                    break
                if ans == "s":
                    continue
                direction = "before" if ans == "b" else "after"
                answers.append((msg_idx, direction))

            if not answers:
                self._queue.put(("log", "No cars selected."))
                self._queue.put(("prompt_batch", "No cars selected"))
                return

            self._batch_num += 1
            scraped_log = scraper.load_scraped_log()
            new_count = 0
            skip_count = 0

            scraper.ensure_dir(scraper.TEST_OUTPUT_DIR)
            self._queue.put(("log", f"Processing {len(answers)} response(s)..."))

            for ans_idx, (msg_idx, direction) in enumerate(answers):
                self._queue.put(("status", f"Processing {ans_idx + 1}/{len(answers)}..."))
                tm = self._messages[msg_idx]

                if scraper.is_already_scraped(tm["text"], scraped_log):
                    self._queue.put(("log", "[~] Already scraped, skipping"))
                    skip_count += 1
                    continue

                car_data = scraper.parse_car_details_regex(tm["text"])
                if not car_data or not car_data.get("is_car_listing"):
                    self._queue.put(("log", "[--] Regex: not a listing, skipping"))
                    skip_count += 1
                    continue

                brand = car_data.get("brand")
                if not brand:
                    self._queue.put(("log", "[!] Missing brand, skipping"))
                    skip_count += 1
                    continue

                price_str = f"P{car_data['price']:,}" if car_data.get("price") else "PON"
                self._queue.put(("log",
                    f"[+] {car_data.get('year','?')} {brand} {car_data.get('model','?')} \u2014 {price_str}"))

                img_msgs = scraper.find_nearest_images(self._messages, msg_idx, direction)
                if not img_msgs:
                    self._queue.put(("log", f"[!] No images {direction}, skipping"))
                    skip_count += 1
                    continue

                seen = set()
                all_urls = []
                all_media = []
                for im in img_msgs:
                    for url, murl in zip(im["image_urls"], im.get("media_urls", [])):
                        if url not in seen:
                            seen.add(url)
                            all_urls.append(url)
                            all_media.append(murl)

                self._queue.put(("log", f"Downloading {len(all_urls)} images..."))
                downloaded = scraper.download_original_images(context, page, all_urls, all_media)

                if not downloaded:
                    self._queue.put(("log", "[!] No images downloaded, skipping"))
                    skip_count += 1
                    continue

                folder = scraper.save_listing_to_dist(car_data, downloaded, scraper.OUTPUT_DIR)
                if folder:
                    scraper.mark_as_scraped(tm["text"], folder, scraped_log)
                    new_count += 1
                    self._queue.put(("log", f"[+] Saved: {folder}"))

            self._queue.put(("log", f"Done: {new_count} new, {skip_count} skipped"))
            self._total_msgs += len(messages)

        except Exception as e:
            self._queue.put(("error", str(e)))
            self._queue.put(("log", traceback.format_exc()))

        self._queue.put(("done",))

    def _start_scrape(self):
        if self._running or not self._browser_ready:
            return
        self._running = True
        self._scrape_btn.configure(state="disabled")
        self._btn_again.pack_forget()
        self._status_var.set("Scraping...")
        self._signal_worker("scrape")

    # ── Debug ───────────────────────────────────────────────

    def _debug_current(self):
        if not self._browser_ready:
            messagebox.showinfo("Debug", "Click Start Browser first.")
            return
        if self._running:
            messagebox.showinfo("Debug", "Wait for the current batch to finish first.")
            return
        self._signal_worker("debug")

    def _do_debug(self, page):
        try:
            info = page.evaluate("""() => {
                const lines = [];
                const regions = document.querySelectorAll('[role="region"]');
                lines.push('Regions: ' + regions.length);
                regions.forEach((r,i) => {
                    const label = r.getAttribute('aria-label') || '';
                    lines.push('  ['+i+'] label="'+label.slice(0,60)+'" imgs='+r.querySelectorAll('img').length);
                });
                const logs = document.querySelectorAll('[role="log"]');
                lines.push('Logs: ' + logs.length);
                logs.forEach((l,i) => lines.push('  ['+i+'] text="'+(l.innerText||'').slice(0,80)+'"'));
                const grids = document.querySelectorAll('[role="grid"]');
                lines.push('Grids: ' + grids.length);
                grids.forEach((g,i) => lines.push('  ['+i+'] label="'+(g.getAttribute('aria-label')||'').slice(0,80)+'" imgs='+g.querySelectorAll('img').length));
                const big = Array.from(document.querySelectorAll('img')).filter(i => i.src.length>40 && i.offsetWidth>=80 && !i.src.includes('emoji') && !i.src.includes('static.xx'));
                lines.push('Big imgs: ' + big.length);
                big.slice(0,4).forEach((img,i) => {
                    let el = img, chain = [];
                    for(let k=0;k<6 && el;k++) { chain.unshift('<'+el.tagName+' role='+(el.getAttribute('role')||'none')+'>'); el = el.parentElement; }
                    lines.push('  img'+(i+1)+': ' + chain.join(' > '));
                });
                return lines.join('\\n');
            }""")
            self._queue.put(("log", "--- DEBUG ---"))
            for line in info.split("\\n"):
                self._queue.put(("log", f"  {line}"))
            self._queue.put(("log", "--- END DEBUG ---"))
        except Exception as e:
            self._queue.put(("error", f"Debug error: {e}"))

    # ── Exit ────────────────────────────────────────────────

    def _on_exit(self):
        if self._running:
            if not messagebox.askokcancel("Exit", "Scraping in progress. Exit anyway?"):
                return
        if self._browser_ready:
            self._signal_worker("exit")
        self.root.after(500, self.root.quit)

    def run(self):
        self.root.mainloop()


if __name__ == "__main__":
    MessengerScraperGUI().run()
