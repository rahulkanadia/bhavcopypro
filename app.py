import os
import pandas as pd
from datetime import datetime
import logging
import threading

from textual import work, on
from textual.app import App, ComposeResult
from textual.widgets import Header, Footer, Input, Checkbox, Button, RichLog, ProgressBar, Label, Select, SelectionList
from textual.widgets.selection_list import Selection
from textual.containers import Horizontal, Vertical

from config import REPORT_TREE
from ledger import load_ledger, check_exists, record_download
from fetcher import MarketFetcher
from file_ops import process_downloaded_file

log_file_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "session_UI.log")
logging.basicConfig(filename=log_file_path, level=logging.INFO, 
                    format='%(asctime)s | %(message)s', datefmt='%H:%M:%S')

class ArchiverApp(App):
    CSS = """
    Screen { background: black; }
    #main_container { layout: horizontal; width: 100%; height: 100%; }
    
    #left_pane { width: 40%; height: 100%; background: $surface; margin-right: 1; padding: 1; }
    #tree_zone { height: 25%; margin-bottom: 1; }
    #list_zone { height: 75%; }
    
    #right_pane { width: 60%; height: 100%; background: $surface; padding: 1; }
    #form_zone { height: auto; margin-bottom: 1; }
    
    #console_zone { height: 1fr; layout: horizontal; }
    #primary_console_container { width: 75%; height: 100%; margin-right: 1; }
    #secondary_console_container { width: 25%; height: 100%; }
    
    .row { layout: horizontal; height: auto; margin-bottom: 1; }
    .list_header_row { layout: horizontal; height: auto; align: left middle; margin-bottom: 1; }
    
    .label { padding-top: 1; width: 25%; color: $text; }
    .inline_label { padding-top: 1; width: auto; padding-left: 1; padding-right: 1; color: $text; }
    .flex_label { padding-top: 1; width: 1fr; color: $text; text-style: bold; }
    .spacer { width: 1fr; }
    
    .input { width: 1fr; }
    .small_input { width: 25%; }
    .btn { margin-right: 2; }
    .btn_small { height: 3; min-width: 15; }
    """
    TITLE = "Bhavcopy Pro"

    def compose(self) -> ComposeResult:
        yield Header()
        
        with Horizontal(id="main_container"):
            # LEFT PANE (40%)
            with Vertical(id="left_pane"):
                with Vertical(id="tree_zone"):
                    yield Label("[b]Select an exchange or sub-section to download bhavcopy for...[/b]\n")
                    yield SelectionList(id="category_list")
                with Vertical(id="list_zone"):
                    with Horizontal(classes="list_header_row"):
                        yield Label("[b]Available Reports[/b]", classes="flex_label")
                        yield Button("Select All", id="btn_select_all", variant="primary", classes="btn_small")
                    yield SelectionList(id="report_list")

            # RIGHT PANE (60%)
            with Vertical(id="right_pane"):
                with Vertical(id="form_zone"):
                    yield Label("[i]Select reports on the left, configure dates below, and start the pull.[/i]\n", classes="row")
                    
                    with Horizontal(classes="row"):
                        yield Label("Save Path:", classes="label")
                        yield Input(placeholder=os.path.dirname(os.path.abspath(__file__)), id="root_dir", classes="input")
                        
                    with Horizontal(classes="row"):
                        yield Label("Start Date:", classes="label")
                        yield Input(placeholder="YYYYMMDD", id="start_date", classes="small_input")
                        yield Label("End Date:", classes="inline_label")
                        yield Input(placeholder="YYYYMMDD", id="end_date", classes="small_input")
                        
                        yield Label(" Day: ", classes="inline_label")
                        yield Select([("All Days", "all"), ("Monday", "0"), ("Tuesday", "1"), 
                                      ("Wednesday", "2"), ("Thursday", "3"), ("Friday", "4")], 
                                      value="all", id="day_select")
                        
                    with Horizontal(classes="row"):
                        yield Label("Specific Dates (comma sep):", classes="label")
                        yield Input(placeholder="e.g. 20240105, 20240809", id="specific_dates", classes="input")

                    with Horizontal(classes="row"):
                        yield Checkbox("Redownload existing files", id="chk_force")
                        yield Checkbox("Unzip archives after download", id="chk_unzip", value=True)
                        yield Label("", classes="spacer")
                        yield Button("Start file pull", id="btn_start", variant="success", classes="btn")
                        yield Button("Interrupt", id="btn_stop", variant="error", classes="btn")

                # PROGRESS & CONSOLES
                with Horizontal(id="console_zone"):
                    with Vertical(id="primary_console_container"):
                        with Horizontal(classes="row"):
                            yield Label("0%", id="progress_pct", classes="inline_label")
                            yield ProgressBar(id="progress_bar", show_eta=False, classes="spacer")
                        yield RichLog(id="log_panel", highlight=True, markup=True)
                        
                    with Vertical(id="secondary_console_container"):
                        yield Button("Copy error logs", id="btn_copy_errors", variant="primary", classes="row")
                        yield RichLog(id="error_log_panel", highlight=True, markup=True)
                    
        yield Footer()

    def on_mount(self) -> None:
        self.error_accumulator = []
        
        # Populate Category List statically (Equity ON by default)
        cat_list = self.query_one("#category_list", SelectionList)
        cat_list.add_option(Selection("NSE Capital Market", "NSE_Capital Market", initial_state=True))
        cat_list.add_option(Selection("NSE Derivatives", "NSE_Derivatives", initial_state=False))
        cat_list.add_option(Selection("BSE Capital Market", "BSE_Capital Market", initial_state=True))
        cat_list.add_option(Selection("BSE Derivatives", "BSE_Derivatives", initial_state=False))
        cat_list.add_option(Selection("BSE Debt", "BSE_Debt", initial_state=False))
        
        self.update_report_list()

    @on(SelectionList.SelectedChanged, "#category_list")
    def on_category_changed(self, event: SelectionList.SelectedChanged) -> None:
        self.update_report_list()

    def update_report_list(self):
        selected_cats = self.query_one("#category_list", SelectionList).selected
        report_list = self.query_one("#report_list", SelectionList)
        report_list.clear_options()
        
        # Dynamically rebuild files list based on categories selected
        for cat in selected_cats:
            exch, seg = cat.split("_")
            reports = REPORT_TREE.get(exch, {}).get(seg, [])
            for rpt in reports:
                # Default to selected
                report_list.add_option(Selection(f"[{exch}] {rpt['name']}", rpt["id"], initial_state=True))

    def dlog(self, ui_markup: str, raw_text: str, is_error: bool = False):
        """Thread-safe Logger. Routes to primary or secondary console."""
        logging.info(raw_text)
        
        def write_ui():
            if is_error:
                self.error_accumulator.append(raw_text)
                self.query_one("#error_log_panel", RichLog).write(ui_markup)
            else:
                self.query_one("#log_panel", RichLog).write(ui_markup)

        try:
            if threading.get_ident() == self._thread_id:
                write_ui()
            else:
                self.call_from_thread(write_ui)
        except RuntimeError:
            write_ui()

    def safe_parse_date(self, d_str):
        clean = d_str.replace("-", "").replace("/", "").strip()
        return datetime.strptime(clean, "%Y%m%d")

    def parse_dates(self):
        specific = self.query_one("#specific_dates", Input).value
        if specific:
            return [self.safe_parse_date(d).strftime('%Y-%m-%d') for d in specific.split(",")]
        
        start_raw = self.query_one("#start_date", Input).value or "20200101"
        end_raw = self.query_one("#end_date", Input).value or datetime.today().strftime('%Y%m%d')
        start = self.safe_parse_date(start_raw)
        end = self.safe_parse_date(end_raw)
        
        day_filter = self.query_one("#day_select", Select).value
        dates = pd.date_range(start=start, end=end, freq='B') 
        
        if day_filter != "all":
            dates = [d for d in dates if d.weekday() == int(day_filter)]
            
        return [d.strftime('%Y-%m-%d') for d in dates]

    @work(thread=True)
    def run_pipeline(self, root_dir, dates, reports, force, unzip):
        progress = self.query_one("#progress_bar", ProgressBar)
        pct_label = self.query_one("#progress_pct", Label)
        
        total_tasks = len(dates) * len(reports)
        completed_tasks = 0
        
        self.call_from_thread(progress.update, total=total_tasks, progress=0)
        self.call_from_thread(pct_label.update, "0%")
        
        ledger = load_ledger()
        fetcher = MarketFetcher()
        self.pipeline_active = True
        self.error_accumulator.clear() # Clear old errors on new run
        
        self.dlog("\n[bold]=== Starting New Run ===[/bold]", "Starting New Run", is_error=True)

        for target_date in dates:
            if not self.pipeline_active:
                self.dlog("[bold red]Pipeline Interrupted by user.[/bold red]", "Pipeline Interrupted.", is_error=True)
                break
                
            for rpt_id in reports:
                if not force and check_exists(ledger, target_date, rpt_id):
                    self.dlog(f"[yellow]SKIP:[/yellow] {target_date} | {rpt_id}", f"SKIP: {target_date} | {rpt_id}")
                else:
                    try:
                        self.dlog(f"[cyan]FETCHING:[/cyan] {target_date} | {rpt_id}", f"FETCHING: {target_date} | {rpt_id}")
                        raw_file_path, exchange, segment, target_sub = fetcher.fetch_report(rpt_id, target_date, root_dir)
                        d_obj = datetime.strptime(target_date, "%Y-%m-%d")
                        final_path = process_downloaded_file(raw_file_path, target_sub, d_obj, exchange, segment, unzip)
                        
                        record_download(target_date, rpt_id, "SUCCESS", final_path)
                        self.dlog(f"[green]SUCCESS:[/green] Saved to {final_path}", f"SUCCESS: Saved to {final_path}")
                        
                    except Exception as e:
                        self.dlog(f"[red]FAILED:[/red] {target_date} | {rpt_id} -> {str(e)}", f"FAILED: {target_date} | {rpt_id} -> {str(e)}", is_error=True)
                        record_download(target_date, rpt_id, "FAILED", "")
                
                # Update Progress
                completed_tasks += 1
                pct = int((completed_tasks / total_tasks) * 100)
                self.call_from_thread(progress.advance)
                self.call_from_thread(pct_label.update, f"{pct}%")

        self.dlog("[bold green]Pipeline Run Complete.[/bold green]", "Pipeline Run Complete.", is_error=True)

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "btn_start":
            root = self.query_one("#root_dir", Input).value or os.path.dirname(os.path.abspath(__file__))
            reports = self.query_one("#report_list", SelectionList).selected
            
            if not reports:
                self.dlog("[red]ERROR: No reports selected.[/red]", "ERROR: No report selected.", is_error=True)
                return
                
            try:
                dates = self.parse_dates()
            except Exception as e:
                self.dlog(f"[red]ERROR: Invalid Date Format.[/red]", f"ERROR: Date parse failed -> {str(e)}", is_error=True)
                return
                
            force = self.query_one("#chk_force", Checkbox).value
            unzip = self.query_one("#chk_unzip", Checkbox).value
            
            self.query_one("#log_panel", RichLog).clear()
            self.query_one("#error_log_panel", RichLog).clear()
            
            self.run_pipeline(root, dates, reports, force, unzip)
            
        elif event.button.id == "btn_select_all":
            self.query_one("#report_list", SelectionList).select_all()
            
        elif event.button.id == "btn_stop":
            self.pipeline_active = False
            self.dlog("[bold yellow]Interrupt signal received. Stopping after current file...[/bold yellow]", "Interrupt signal received.", is_error=True)
            
        elif event.button.id == "btn_copy_errors":
            if self.error_accumulator:
                error_text = "\n".join(self.error_accumulator)
                self.app.clipboard = error_text
                self.notify("Error logs copied to clipboard!", title="Success", timeout=3)
            else:
                self.notify("No errors to copy.", title="Info", timeout=3)

if __name__ == "__main__":
    app = ArchiverApp()
    app.run()