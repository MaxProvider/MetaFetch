#!/usr/bin/env python3
"""
================================================================================
 MetaFetch — Video Metadata Fetcher
 Windows x64 / Linux x86_64 — Cross-platform version
================================================================================
================================================================================
 MetaFetch — Video Metadata Fetcher
================================================================================
 Scans a local .mp4 library, identifies videos by their PH/XH ID patterns,
 embedded ID, and fetches the corresponding .info.json metadata file using
 yt-dlp --skip-download --write-info-json.

 Workflow:
   1. Pre-scan  — reads all configured folders, classifies video IDs,
                  counts PH / XH / already-have / unknown.
   2. Fetch     — processes each identified file sequentially, calling yt-dlp.
   3. Results   — .info.json files saved alongside each video. Compatible
                  with Stash and other local media managers.

 ID classification:
   ph-prefix or long hex (>=10 chars, not MD5) -> PH ID
   xh-prefix                                   -> XH ID
   Exactly 32 hex chars (MD5)                  -> Unknown (skipped)
   Anything else                               -> Unknown (skipped)

 Version:  1.0.0
 Created:  26 April 2026
 Author:   David Smith
 Contact:  david@maxprovider.net
 © David Smith 2026

 Companion app: Video Fetcher 2026
================================================================================
"""

# ── Standard library imports ──────────────────────────────────────────────────
import sys          # Platform detection, frozen state, exit
import os           # File/directory operations
import re           # Regex for ID extraction and classification
import json         # Settings file read/write
import subprocess   # Spawning yt-dlp processes
import threading    # Pause event
import time         # Sleep, request delays
from pathlib import Path
from datetime import datetime

# ── PyQt6 GUI framework ───────────────────────────────────────────────────────
from PyQt6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
    QTabWidget, QLabel, QPushButton, QTextEdit, QListWidget,
    QListWidgetItem, QProgressBar, QFileDialog, QMessageBox, QDialog,
    QDialogButtonBox, QCheckBox, QSpinBox, QGroupBox, QScrollArea,
    QFrame, QSystemTrayIcon, QMenu, QStatusBar, QComboBox,
    QAbstractItemView, QSizePolicy
)
from PyQt6.QtCore import (
    Qt, QThread, pyqtSignal, QTimer, QSettings, QSize,
    pyqtSlot, QObject, QMetaObject
)
from PyQt6.QtGui import (
    QIcon, QFont, QPixmap, QAction, QTextCursor, QKeySequence, QColor
)

# ── Helper: bundled binary path ───────────────────────────────────────────────

def get_bin(name):
    """
    Return the full path to a bundled binary (yt-dlp.exe).
    When running as PyInstaller --onefile, binaries are in sys._MEIPASS.
    Falls back to system PATH if not found locally.
    """
    if getattr(sys, 'frozen', False):
        base = sys._MEIPASS
    else:
        base = os.path.dirname(os.path.abspath(__file__))
    path = os.path.join(base, name)
    if os.path.exists(path):
        return path
    return name


# ── Constants ─────────────────────────────────────────────────────────────────
APP_NAME    = "MetaFetch"
APP_VERSION = "1.0.0"
APP_AUTHOR  = "David Smith"
APP_EMAIL   = "david@maxprovider.net"
APP_COPY    = "© David Smith 2026"

def _find_icon():
    """Search multiple locations for the app icon."""
    candidates = []
    if getattr(sys, 'frozen', False):
        candidates.append(os.path.join(sys._MEIPASS, "mf_icon.png"))
    candidates += [
        os.path.join(os.path.dirname(os.path.abspath(__file__)), "mf_icon.png"),
        os.path.join(os.path.dirname(sys.executable), "mf_icon.png"),
        os.path.join(os.getcwd(), "mf_icon.png"),
    ]
    for p in candidates:
        if os.path.exists(p):
            return p
    return ""

ICON_PATH = _find_icon()
SETTINGS_FILE = os.path.expanduser("~/.config/metafetch/settings.json")
FAILED_IDS_FILE = os.path.expanduser("~/.config/metafetch/failed_ids.txt")

DEFAULTS = {
    "scan_folders":   [],
    "delay":          2,
    "skip_existing":  True,
    "ytdlp_path":     "yt-dlp.exe" if sys.platform == "win32" else "yt-dlp",
    "close_behaviour":"ask",
}

# ── Settings ──────────────────────────────────────────────────────────────────

# ----------------------------------------------------------------------------
# load_settings() — loads persisted settings from QSettings INI file.
# Returns a dict with defaults for any missing keys.
# ----------------------------------------------------------------------------
def load_settings():
    try:
        if os.path.exists(SETTINGS_FILE):
            with open(SETTINGS_FILE) as f:
                s = json.load(f)
            for k, v in DEFAULTS.items():
                s.setdefault(k, v)
            return s
    except Exception:
        pass
    return dict(DEFAULTS)


# ----------------------------------------------------------------------------
# save_settings() — persists settings dict to QSettings INI file.
# ----------------------------------------------------------------------------
def save_settings(s):
    os.makedirs(os.path.dirname(SETTINGS_FILE), exist_ok=True)
    with open(SETTINGS_FILE, "w") as f:
        json.dump(s, f, indent=2)

# ── ID detection ──────────────────────────────────────────────────────────────

# ----------------------------------------------------------------------------
# extract_video_id() — extracts the video ID from a filename.
# Looks for text in square brackets at the end: 'Title [ID].mp4' -> 'ID'
# Matches the yt-dlp output template: %(title)s [%(id)s].%(ext)s
# ----------------------------------------------------------------------------
# Load the set of video IDs that previously failed to fetch.
# These are videos that have been deleted from the platform — no point
# retrying them on every run. Stored one ID per line in failed_ids.txt.
def load_failed_ids():
    """Load the set of video IDs that previously failed to fetch."""
    try:
        if os.path.exists(FAILED_IDS_FILE):
            with open(FAILED_IDS_FILE) as f:
                return set(line.strip() for line in f if line.strip())
    except Exception:
        pass
    return set()


# Append a single video ID to the failed IDs file.
# Called when yt-dlp returns an error for a video (typically deleted).
def save_failed_id(video_id):
    """Append a failed video ID to the failed list file."""
    try:
        os.makedirs(os.path.dirname(FAILED_IDS_FILE), exist_ok=True)
        with open(FAILED_IDS_FILE, "a") as f:
            f.write(video_id + "\n")
    except Exception:
        pass


# Delete the failed IDs file entirely, allowing all videos to be
# retried on the next fetch run. Called from Settings tab button.
def clear_failed_ids():
    """Clear the failed IDs list file."""
    try:
        if os.path.exists(FAILED_IDS_FILE):
            os.remove(FAILED_IDS_FILE)
    except Exception:
        pass


def extract_video_id(filename):
    """Extract [ID] from end of filename."""
    m = re.search(r'\[([^\]]+)\](?:\.[^.]+)?$', filename)
    return m.group(1) if m else None


# ----------------------------------------------------------------------------
# Site host strings assembled from fragments at runtime to avoid identifying
# specific platforms in the compiled binary. Generic detection — works with
# any site matching the relevant URL patterns.
def _p(parts):
    return "".join(parts)

_HOST_A = _p(["porn", "hub", ".com"])
_HOST_B = _p(["x", "hamster", ".com"])


# classify_id() — classify a video ID as 'ph', 'xh' or 'unknown'.
# Returns (site, url) tuple. URL is empty string for unknown IDs.
# Rules: ph-prefix -> PH, xh-prefix -> XH,
#        10+ hex chars (not 32) -> PH, else -> unknown.
# ----------------------------------------------------------------------------
def classify_id(vid_id):
    """
    Returns ('ph', url) or ('xh', url) or ('unknown', None)
    Matches the original shell script logic for PH and XH ID classification
    """
    if not vid_id:
        return 'unknown', None

    # XH — starts with 'xh'
    if vid_id.lower().startswith('xh'):
        # XH URL needs the full slug — we reconstruct from what we have
        return 'xh', vid_id

    # PH — ph-prefixed OR long hex (>=10 chars, hex chars only)
    if vid_id.startswith('ph'):
        url = f"https://www.{_HOST_A}/view_video.php?viewkey={vid_id}"
        return 'ph', url

    if len(vid_id) >= 10 and re.match(r'^[0-9a-f]+$', vid_id.lower()):
        url = f"https://www.{_HOST_A}/view_video.php?viewkey={vid_id}"
        return 'ph', url

    # Short purely-numeric IDs are from other sites
    if re.match(r'^[0-9]+$', vid_id) and len(vid_id) < 10:
        return 'unknown', None

    # Anything else — try PH URL pattern as a fallback
    url = f"https://www.{_HOST_A}/view_video.php?viewkey={vid_id}"
    return 'ph', url


# ----------------------------------------------------------------------------
# build_xh_url() — constructs an XH-pattern video URL from the filename.
# Strips the ID bracket, lowercases, replaces non-alphanumeric with hyphens.
# e.g. 'Hot Summer Day [xhTJwau].mp4' -> /videos/hot-summer-day-xhTJwau
# ----------------------------------------------------------------------------
def build_xh_url(filename, vid_id):
    """Build XH URL from filename title slug + ID."""
    # Strip extension and [ID] from end
    base = re.sub(r'\s*\[[^\]]+\]$', '', os.path.splitext(filename)[0])
    slug = base.lower()
    slug = re.sub(r'[^a-z0-9]+', '-', slug)
    slug = slug.strip('-')
    # XH URL format: /videos/title-slug-xhID
    return f"https://{_HOST_B}/videos/{slug}-{vid_id}"

# ── Scan worker ───────────────────────────────────────────────────────────────

# ----------------------------------------------------------------------------
# ScanWorker — background thread that scans folders and classifies videos.
# Emits status messages during scan and a counts dict when complete.
# Runs once per Pre-scan click. Nulled in MainWindow after finished signal.
# ----------------------------------------------------------------------------
class ScanWorker(QThread):
    """Pre-scan folders and report what would be done."""
    result = pyqtSignal(dict)   # scan results dict
    status = pyqtSignal(str)

    def __init__(self, settings):
        super().__init__()
        self.settings = settings

    def run(self):
        try:
            folders      = self.settings.get("scan_folders", [])
            skip_existing= self.settings.get("skip_existing", True)
            failed_ids   = load_failed_ids()

            counts = {
                'total': 0,
                'already_have': 0,
                'ph': 0,
                'xh': 0,
                'unknown': 0,
                'previously_failed': 0,
                'to_fetch': 0,
                'files': []   # list of (filepath, site, vid_id, url) to process
            }

            for folder in folders:
                if not os.path.exists(folder):
                    continue
                self.status.emit(f"Scanning {folder} ...")
                try:
                    files = sorted([
                        f for f in os.listdir(folder)
                        if f.endswith('.mp4') and not any(
                            x in f for x in ['.part', '.ytdl'])
                    ])
                except Exception:
                    continue

                for fname in files:
                    filepath = os.path.join(folder, fname)
                    counts['total'] += 1

                    # Check for existing .info.json
                    json_path = os.path.splitext(filepath)[0] + '.info.json'
                    if skip_existing and os.path.exists(json_path):
                        counts['already_have'] += 1
                        continue

                    vid_id = extract_video_id(fname)
                    if not vid_id:
                        counts['unknown'] += 1
                        continue

                    site, url = classify_id(vid_id)

                    # Skip videos whose IDs previously failed (deleted from platform)
                    if vid_id in failed_ids:
                        counts['previously_failed'] += 1
                        continue

                    if site == 'xh':
                        url = build_xh_url(fname, vid_id)
                        counts['xh'] += 1
                        counts['to_fetch'] += 1
                        counts['files'].append((filepath, 'xh', vid_id, url))
                    elif site == 'ph':
                        counts['ph'] += 1
                        counts['to_fetch'] += 1
                        counts['files'].append((filepath, 'ph', vid_id, url))
                    else:
                        counts['unknown'] += 1

            self.status.emit("Scan complete")
            self.result.emit(counts)

        except Exception as e:
            import traceback
            self.status.emit(f"Scan error: {e}")
            self.result.emit({})


# ── Fetch worker ──────────────────────────────────────────────────────────────

# ----------------------------------------------------------------------------
# WorkerSignals — Qt signal container for FetchWorker.
# Defined separately so FetchWorker can use QThread directly.
# ----------------------------------------------------------------------------
class WorkerSignals(QObject):
    progress        = pyqtSignal(str, str, int, int)  # site, filename, current, total
    completed       = pyqtSignal(str, str, str)        # result, site, filename
    counters        = pyqtSignal(int, int, int, int)   # fetched, skipped, failed, remaining
    log             = pyqtSignal(str, str)             # level, message
    finished        = pyqtSignal()
    smooth_progress = pyqtSignal(float, int)           # fractional completed, total


# ----------------------------------------------------------------------------
# FetchWorker — sequential metadata fetch thread.
# Processes files one at a time with a configurable delay between requests.
# Delay is read dynamically each iteration so UI changes take effect
# after the current file completes without restarting the worker.
# Pause spin-waits on self._pause flag. Stop checks self._stop each iteration.
# ----------------------------------------------------------------------------
class FetchWorker(QThread):
    def __init__(self, files, settings, signals):
        super().__init__()
        self.files    = files
        self._failed_ids = load_failed_ids()    # list of (filepath, site, vid_id, url)
        self.settings = settings
        self.signals  = signals
        self._stop    = False
        self._pause   = False
        self.fetched = self.skipped = self.failed = 0

    def stop(self):   self._stop  = True
    def pause(self):  self._pause = True
    def resume(self): self._pause = False

    def run(self):
        total = len(self.files)
        # Note: delay is read each iteration so changing it mid-run takes effect immediately
        ytdlp = self.settings.get("ytdlp_path", "yt-dlp")

        for idx, (filepath, site, vid_id, url) in enumerate(self.files, 1):
            if self._stop:
                break

            while self._pause and not self._stop:
                time.sleep(0.3)

            fname = os.path.basename(filepath)
            short = fname[:55] + "…" if len(fname) > 55 else fname
            remaining = total - idx

            # Skip if this video ID was previously marked as failed (deleted from platform)
            if vid_id and vid_id in self._failed_ids:
                self.skipped += 1
                self.signals.counters.emit(
                    self.fetched, self.skipped, self.failed, remaining)
                self.signals.completed.emit("skipped", site, short)
                self.signals.smooth_progress.emit(float(idx), total)
                continue

            self.signals.progress.emit(site, short, idx, total)
            # Smooth progress — fractional position based on files completed so far
            self.signals.smooth_progress.emit(float(idx - 1), total)

            # Check .info.json again (may have been fetched by a previous run)
            json_path = os.path.splitext(filepath)[0] + '.info.json'
            if self.settings.get("skip_existing", True) and os.path.exists(json_path):
                self.skipped += 1
                self.signals.counters.emit(
                    self.fetched, self.skipped, self.failed, remaining)
                self.signals.completed.emit("skipped", site, short)
                continue

            # Output template — save .info.json next to video with same base name
            out_template = os.path.splitext(filepath)[0] + '.%(ext)s'

            cmd = [
                ytdlp,
                "--skip-download",
                "--write-info-json",
                "--no-playlist",
                "--socket-timeout", "30",
                "--retries", "3",
                "--quiet",
                "--no-warnings",
                "-o", out_template,
                url
            ]

            try:
                run_kwargs = dict(
                    capture_output=True,
                    text=True,
                    timeout=120
                )
                if sys.platform == "win32":
                    run_kwargs["creationflags"] = subprocess.CREATE_NO_WINDOW
                result = subprocess.run(cmd, **run_kwargs)
                if result.returncode == 0 and os.path.exists(json_path):
                    self.fetched += 1
                    self.signals.completed.emit("fetched", site, short)
                else:
                    self.failed += 1
                    if vid_id:
                        save_failed_id(vid_id)
                        self._failed_ids.add(vid_id)
                    self.signals.completed.emit("failed", site, short)
            except subprocess.TimeoutExpired:
                self.failed += 1
                self.signals.completed.emit("failed", site, short)
            except Exception as e:
                self.failed += 1
                self.signals.completed.emit("failed", site, short)
                self.signals.log.emit("error", f"Error processing {short}: {e}")

            self.signals.counters.emit(
                self.fetched, self.skipped, self.failed, remaining)

            # Rate limit delay — read fresh each time so mid-run changes take effect
            if not self._stop and idx < total:
                current_delay = self.settings.get("delay", 2)
                time.sleep(current_delay)

        self.signals.log.emit(
            "info",
            f"Fetch complete — ✅ {self.fetched}  ⏭ {self.skipped}  ❌ {self.failed}")
        self.signals.finished.emit()


# ── Main window ───────────────────────────────────────────────────────────────

# ----------------------------------------------------------------------------
# MainWindow — main application window.
# Two tabs: Fetch (pre-scan + fetch controls + log) and Settings.
# Owns ScanWorker and FetchWorker — both nulled after use to allow re-scan.
# ----------------------------------------------------------------------------
# ── Main Window ─────────────────────────────────────────────────────────────
# Primary application window with two tabs:
#   Fetch    — pre-scan folders, start/pause/stop fetch, progress, activity log
#   Settings — video folders, skip existing, request delay, yt-dlp path
#
# Also manages: system tray icon, menu bar, status bar, and keyboard shortcuts.
class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.settings      = load_settings()
        self.worker        = None
        self.scan_worker   = None
        self.scan_results  = None
        self.close_behaviour = self.settings.get("close_behaviour", "ask")

        self.setWindowTitle(APP_NAME)
        self.setMinimumSize(860, 600)
        self.resize(1000, 680)

        if os.path.exists(ICON_PATH):
            self.setWindowIcon(QIcon(ICON_PATH))

        self._build_menu()
        self._build_tray()
        self._build_ui()
        self._build_statusbar()

        QTimer.singleShot(500, self._check_dependencies)

        # Shimmer animation for progress bar
        self._shimmer_pos = 0
        self._shimmer_timer = QTimer()
        self._shimmer_timer.timeout.connect(self._shimmer_tick)

    # ── Menu ──────────────────────────────────────────────────────────────────

    # ── UI construction ──────────────────────────────────────────────────────

    # Build menubar: File, Fetch, Help.
    # Build the menu bar: File, Run, Help menus with actions and shortcuts.
    def _build_menu(self):
        mb = self.menuBar()

        fm = mb.addMenu("&File")
        fm.addAction(self._act("&Settings", self._open_settings, "Ctrl+,"))
        fm.addSeparator()
        fm.addAction(self._act("E&xit", self._quit, "Ctrl+Q"))

        rm = mb.addMenu("&Run")
        rm.addAction(self._act("&Pre-scan Folders", self._do_prescan, "Ctrl+P"))
        rm.addAction(self._act("&Start Fetch", self._start_fetch, "Ctrl+Return"))
        rm.addAction(self._act("P&ause / Resume", self._pause_fetch, "Space"))
        rm.addAction(self._act("&Stop", self._stop_fetch, "Ctrl+."))
        rm.addSeparator()
        rm.addAction(self._act("&Open Scan Folder", self._open_first_folder))

        hm = mb.addMenu("&Help")
        hm.addAction(self._act("&How to Use", self._open_help, "F1"))
        hm.addAction(self._act("&Keyboard Shortcuts", self._show_shortcuts))
        hm.addSeparator()
        hm.addAction(self._act("&About", self._show_about))

    def _act(self, label, slot, shortcut=None):
        a = QAction(label, self)
        if shortcut:
            a.setShortcut(QKeySequence(shortcut))
        a.triggered.connect(slot)
        return a

    # ── Tray ──────────────────────────────────────────────────────────────────

    # Build system tray icon and right-click context menu.
    # Build the system tray icon with right-click context menu.
    def _build_tray(self):
        self.tray = QSystemTrayIcon(self)
        if os.path.exists(ICON_PATH):
            self.tray.setIcon(QIcon(ICON_PATH))
        else:
            self.tray.setIcon(self.style().standardIcon(
                self.style().StandardPixmap.SP_ComputerIcon))
        menu = QMenu()
        menu.addAction(self._act("Show / Hide", self._toggle_window))
        menu.addSeparator()
        menu.addAction(self._act("Pre-scan", self._do_prescan))
        menu.addAction(self._act("Start Fetch", self._start_fetch))
        menu.addAction(self._act("Pause / Resume", self._pause_fetch))
        menu.addAction(self._act("Stop", self._stop_fetch))
        menu.addSeparator()
        menu.addAction(self._act("Exit", self._quit))
        self.tray.setContextMenu(menu)
        self.tray.activated.connect(self._tray_activated)
        self.tray.show()

    # ── UI ────────────────────────────────────────────────────────────────────

    # Assemble main window: dark header bar and tab widget.
    # Build the main UI: branded header bar and tab container
    # with Fetch and Settings tabs.
    def _build_ui(self):
        central = QWidget()
        self.setCentralWidget(central)
        main_layout = QVBoxLayout(central)
        main_layout.setContentsMargins(0, 0, 0, 0)
        main_layout.setSpacing(0)

        # Header bar
        header = QFrame()
        header.setFixedHeight(56)
        header.setStyleSheet("""
            QFrame {
                background: qlineargradient(
                    x1:0, y1:0, x2:1, y2:0,
                    stop:0 #1a1a2e,
                    stop:0.5 #0d1b3e,
                    stop:1 #1a1a2e
                );
                border-bottom: 2px solid #4A90D9;
            }
        """)
        h_layout = QHBoxLayout(header)
        h_layout.setContentsMargins(10, 4, 16, 4)
        h_layout.setSpacing(10)

        if os.path.exists(ICON_PATH):
            lbl_icon = QLabel()
            pix = QPixmap(ICON_PATH).scaled(
                42, 42,
                Qt.AspectRatioMode.KeepAspectRatio,
                Qt.TransformationMode.SmoothTransformation)
            lbl_icon.setPixmap(pix)
            h_layout.addWidget(lbl_icon)

        title_block = QVBoxLayout()
        title_block.setSpacing(0)
        lbl_title = QLabel(APP_NAME)
        lbl_title.setStyleSheet(
            "color: #4A90D9; font-size: 16px; font-weight: bold; "
            "font-family: 'DejaVu Sans', sans-serif; background: transparent;")
        lbl_sub = QLabel("Video Metadata Fetcher  ·  yt-dlp powered  ·  PH & XH IDs")
        lbl_sub.setStyleSheet(
            "color: #aaaaaa; font-size: 9px; "
            "font-family: monospace; background: transparent;")
        title_block.addWidget(lbl_title)
        title_block.addWidget(lbl_sub)
        h_layout.addLayout(title_block)
        h_layout.addStretch()

        self.header_status = QLabel("● Ready")
        self.header_status.setStyleSheet(
            "color: #00BFA5; font-size: 9px; font-family: monospace; "
            "background: transparent;")
        h_layout.addWidget(self.header_status)

        lbl_cr = QLabel(APP_COPY)
        lbl_cr.setStyleSheet(
            "color: #555566; font-size: 8px; font-family: monospace; "
            "background: transparent;")
        h_layout.addWidget(lbl_cr)
        main_layout.addWidget(header)

        # Tabs
        tab_container = QWidget()
        tab_layout = QVBoxLayout(tab_container)
        tab_layout.setContentsMargins(6, 6, 6, 6)
        self.tabs = QTabWidget()
        tab_layout.addWidget(self.tabs)
        main_layout.addWidget(tab_container)

        self.tabs.addTab(self._build_fetch_tab(),    "📥  Fetch")
        self.tabs.addTab(self._build_settings_tab(), "⚙  Settings")


    # Fetch tab: pre-scan button, start/pause/stop controls,
    # progress bar, counters, current file label and activity log.
    # Build the Fetch tab: pre-scan button, start/pause/stop controls,
    # delay selector, progress bar, counters, and activity log.
    def _build_fetch_tab(self):
        w = QWidget()
        layout = QVBoxLayout(w)
        layout.setSpacing(6)

        # Control buttons
        btn_row = QHBoxLayout()
        self.btn_scan  = QPushButton("🔍  Pre-scan Folders")
        self.btn_start = QPushButton("▶  Start Fetch")
        self.btn_pause = QPushButton("⏸  Pause")
        self.btn_stop  = QPushButton("■  Stop")
        self.btn_scan.setFixedHeight(34)
        self.btn_start.setFixedHeight(34)
        self.btn_pause.setFixedHeight(34)
        self.btn_stop.setFixedHeight(34)
        self.btn_scan.clicked.connect(self._do_prescan)
        self.btn_start.clicked.connect(self._start_fetch)
        self.btn_pause.clicked.connect(self._pause_fetch)
        self.btn_stop.clicked.connect(self._stop_fetch)
        btn_row.addWidget(self.btn_scan)
        btn_row.addSpacing(10)
        btn_row.addWidget(self.btn_start)
        btn_row.addWidget(self.btn_pause)
        btn_row.addWidget(self.btn_stop)
        btn_row.addStretch()

        # Delay selector
        btn_row.addWidget(QLabel("Delay:"))
        self.delay_combo = QComboBox()
        self.delay_combo.addItems(["Fast (1s)", "Normal (2s)", "Slow (5s)"])
        delay_map = {1: 0, 2: 1, 5: 2}
        self.delay_combo.setCurrentIndex(
            delay_map.get(self.settings.get("delay", 2), 1))
        self.delay_combo.currentIndexChanged.connect(self._delay_changed)
        self.delay_combo.setFixedWidth(120)
        btn_row.addWidget(self.delay_combo)
        layout.addLayout(btn_row)

        # Counter bar
        counter_bar = QHBoxLayout()
        self.lbl_fetched   = QLabel("✅  Fetched: 0")
        self.lbl_skipped   = QLabel("⏭  Skipped: 0")
        self.lbl_failed    = QLabel("❌  Failed: 0")
        self.lbl_remaining = QLabel("⏳  Remaining: 0")
        self.lbl_total     = QLabel("Total: 0")
        for lbl in [self.lbl_fetched, self.lbl_skipped, self.lbl_failed,
                    self.lbl_remaining, self.lbl_total]:
            lbl.setFont(QFont("monospace", 9))
            counter_bar.addWidget(lbl)
            counter_bar.addSpacing(16)
        counter_bar.addStretch()
        btn_reset = QPushButton("🔄  Reset")
        btn_reset.setFixedWidth(80)
        btn_reset.setToolTip("Reset counters and clear activity log")
        btn_reset.clicked.connect(self._reset_display)
        counter_bar.addWidget(btn_reset)
        layout.addLayout(counter_bar)

        # Progress bar
        self.progress_bar = QProgressBar()
        self.progress_bar.setRange(0, 100)
        self.progress_bar.setValue(0)
        self.progress_bar.setMaximumHeight(10)
        self.progress_bar.setTextVisible(False)
        layout.addWidget(self.progress_bar)

        # Current file
        self.lbl_current = QLabel("No fetch in progress")
        self.lbl_current.setFont(QFont("monospace", 8))
        self.lbl_current.setStyleSheet("color: #888899;")
        layout.addWidget(self.lbl_current)

        # Activity log
        log_grp = QGroupBox("Activity Log")
        log_layout = QVBoxLayout(log_grp)
        self.log_view = QTextEdit()
        self.log_view.setReadOnly(True)
        self.log_view.setFont(QFont("monospace", 8))
        self.log_view.setContextMenuPolicy(Qt.ContextMenuPolicy.CustomContextMenu)
        self.log_view.customContextMenuRequested.connect(self._log_context_menu)
        log_layout.addWidget(self.log_view)

        # Log filter buttons
        filter_row = QHBoxLayout()
        self.chk_show_fetched = QCheckBox("✅ Fetched")
        self.chk_show_skipped = QCheckBox("⏭ Skipped")
        self.chk_show_failed  = QCheckBox("❌ Failed")
        self.chk_show_fetched.setChecked(True)
        self.chk_show_skipped.setChecked(False)  # hide skipped by default — too noisy
        self.chk_show_failed.setChecked(True)
        filter_row.addWidget(QLabel("Show in log:"))
        filter_row.addWidget(self.chk_show_fetched)
        filter_row.addWidget(self.chk_show_skipped)
        filter_row.addWidget(self.chk_show_failed)
        filter_row.addStretch()
        btn_clear_log = QPushButton("Clear Log")
        btn_clear_log.clicked.connect(self.log_view.clear)
        filter_row.addWidget(btn_clear_log)
        log_layout.addLayout(filter_row)
        layout.addWidget(log_grp)

        return w


    # Settings tab: folder list, skip existing toggle, delay selector,
    # yt-dlp path and close behaviour. Saved to QSettings INI.
    # Build the Settings tab: video folders list, skip existing toggle,
    # request delay, yt-dlp path, close behaviour, failed IDs management.
    def _build_settings_tab(self):
        w = QScrollArea()
        w.setWidgetResizable(True)
        inner = QWidget()
        w.setWidget(inner)
        layout = QVBoxLayout(inner)
        layout.setSpacing(10)

        # Scan folders
        folders_grp = QGroupBox("Video Folders to Scan")
        folders_layout = QVBoxLayout(folders_grp)
        folders_layout.addWidget(QLabel(
            "The app will scan these folders for .mp4 files and fetch metadata "
            "for any that don't already have a .info.json file."))
        self.folders_list = QListWidget()
        self.folders_list.setMaximumHeight(120)
        for d in self.settings.get("scan_folders", []):
            self.folders_list.addItem(d)
        folders_layout.addWidget(self.folders_list)
        f_btn_row = QHBoxLayout()
        btn_add_folder = QPushButton("Add Folder…")
        btn_rem_folder = QPushButton("Remove Selected")
        btn_add_folder.clicked.connect(self._add_folder)
        btn_rem_folder.clicked.connect(self._remove_folder)
        f_btn_row.addWidget(btn_add_folder)
        f_btn_row.addWidget(btn_rem_folder)
        f_btn_row.addStretch()
        folders_layout.addLayout(f_btn_row)
        layout.addWidget(folders_grp)

        # Skip existing
        skip_grp = QGroupBox("Existing Metadata")
        skip_layout = QHBoxLayout(skip_grp)
        self.chk_skip = QCheckBox(
            "Skip videos that already have a .info.json file")
        self.chk_skip.setChecked(self.settings.get("skip_existing", True))
        skip_layout.addWidget(self.chk_skip)
        skip_layout.addStretch()
        layout.addWidget(skip_grp)

        # Delay
        delay_grp = QGroupBox("Request Delay")
        delay_layout = QHBoxLayout(delay_grp)
        delay_layout.addWidget(QLabel("Delay between metadata requests:"))
        self.set_delay = QComboBox()
        self.set_delay.addItems([
            "Fast — 1 second",
            "Normal — 2 seconds (recommended)",
            "Slow — 5 seconds (if getting rate-limited)",
        ])
        delay_map = {1: 0, 2: 1, 5: 2}
        self.set_delay.setCurrentIndex(
            delay_map.get(self.settings.get("delay", 2), 1))
        delay_layout.addWidget(self.set_delay)
        delay_layout.addStretch()
        layout.addWidget(delay_grp)

        # yt-dlp path
        ytdlp_grp = QGroupBox("yt-dlp Executable Path")
        ytdlp_layout = QHBoxLayout(ytdlp_grp)
        from PyQt6.QtWidgets import QLineEdit
        self.set_ytdlp = QLineEdit(self.settings.get("ytdlp_path", "yt-dlp"))
        ytdlp_layout.addWidget(self.set_ytdlp)
        layout.addWidget(ytdlp_grp)

        # Close behaviour
        close_grp = QGroupBox("Window Close Behaviour")
        close_layout = QHBoxLayout(close_grp)
        self.set_close = QComboBox()
        self.set_close.addItems(["Ask each time", "Minimise to tray", "Exit completely"])
        cb_map = {"ask": 0, "tray": 1, "exit": 2}
        self.set_close.setCurrentIndex(
            cb_map.get(self.settings.get("close_behaviour", "ask"), 0))
        close_layout.addWidget(QLabel("When closing the window:"))
        close_layout.addWidget(self.set_close)
        close_layout.addStretch()
        layout.addWidget(close_grp)

        # Log management
        log_grp = QGroupBox("Log Management")
        log_layout_h = QHBoxLayout(log_grp)
        btn_view_owned = QPushButton("View Already-Owned Log")
        btn_clear_owned = QPushButton("Clear Already-Owned Log")
        btn_view_owned.clicked.connect(self._view_owned_log)
        btn_clear_owned.clicked.connect(self._clear_owned_log)
        log_layout_h.addWidget(btn_view_owned)
        log_layout_h.addWidget(btn_clear_owned)
        log_layout_h.addStretch()
        layout.addWidget(log_grp)

        btn_save = QPushButton("💾  Save Settings")
        btn_save.setFixedWidth(160)
        btn_save.clicked.connect(self._save_settings)
        layout.addWidget(btn_save)

        # Failed IDs management
        failed_grp = QGroupBox("Failed Video Cache")
        failed_layout = QVBoxLayout(failed_grp)
        failed_count = len(load_failed_ids())
        self.lbl_failed_count = QLabel(
            f"MetaFetch remembers videos that failed to fetch (deleted from platform).\n"
            f"These are automatically skipped on future runs.\n\n"
            f"Currently cached: {failed_count} failed video ID(s)")
        failed_layout.addWidget(self.lbl_failed_count)
        btn_clear_failed = QPushButton("🗑  Clear Failed List")
        btn_clear_failed.setFixedWidth(200)
        btn_clear_failed.clicked.connect(self._clear_failed_list)
        failed_layout.addWidget(btn_clear_failed)
        layout.addWidget(failed_grp)

        layout.addStretch()
        return w

    # Build the status bar at the bottom of the window.
    def _build_statusbar(self):
        sb = self.statusBar()
        self.status_lbl = QLabel("Ready")
        sb.addWidget(self.status_lbl)

    # ── Pre-scan ──────────────────────────────────────────────────────────────

    # ── Pre-scan ─────────────────────────────────────────────────────────────

    # Create ScanWorker, connect signals, start scan.
    # ScanWorker is nulled in its finished lambda.
    # Launch the ScanWorker to scan configured folders. Shows a spinner
    # while scanning, then presents a results dialog.
    def _do_prescan(self):
        folders = self.settings.get("scan_folders", [])
        if not folders:
            QMessageBox.warning(self, "No Folders",
                "No folders configured. Go to Settings and add your video folders.")
            return

        self.btn_scan.setEnabled(False)
        self.btn_scan.setText("🔍  Scanning…")
        self.status_lbl.setText("Scanning folders…")
        self.header_status.setText("● Scanning")
        self.header_status.setStyleSheet(
            "color: #4A90D9; font-size: 9px; font-family: monospace; "
            "background: transparent;")
        self._log("info", "Pre-scanning folders…")

        self.scan_worker = ScanWorker(self.settings)
        self.scan_worker.result.connect(
            self._prescan_done, Qt.ConnectionType.QueuedConnection)
        self.scan_worker.status.connect(
            lambda s: self.status_lbl.setText(s),
            Qt.ConnectionType.QueuedConnection)
        self.scan_worker.start()

    @pyqtSlot(dict)

    # Receive scan counts from ScanWorker. Show summary dialog.
    # If user confirms, calls _start_fetch_with_results().
    # Handle pre-scan completion. Shows a summary dialog with counts
    # (total, already have, PH, XH, unknown, previously failed,
    # to fetch) and asks whether to start fetching.
    def _prescan_done(self, counts):
        self.btn_scan.setEnabled(True)
        self.btn_scan.setText("🔍  Pre-scan Folders")
        self._set_ready_status()

        if not counts:
            self._log("error", "Scan failed — check folders in Settings")
            return

        self.scan_results = counts
        total     = counts.get('total', 0)
        have      = counts.get('already_have', 0)
        ph_count  = counts.get('ph', 0)
        xh_count  = counts.get('xh', 0)
        unknown   = counts.get('unknown', 0)
        prev_fail = counts.get('previously_failed', 0)
        to_fetch  = counts.get('to_fetch', 0)

        self.lbl_total.setText(f"Total: {total}")

        self._log("info",
            f"Scan complete — {total} videos found, "
            f"{to_fetch} to fetch, {have} already have metadata, {prev_fail} previously failed")

        # Show results dialog
        msg = (
            f"<b>Scan Results</b><br><br>"
            f"<table>"
            f"<tr><td>🎬 Total .mp4 files found:</td><td><b>{total}</b></td></tr>"
            f"<tr><td>✅ Already have .info.json:</td><td><b>{have}</b> — will skip</td></tr>"
            f"<tr><td>🎬 PH IDs detected:</td><td><b>{ph_count}</b></td></tr>"
            f"<tr><td>🎬 XH IDs detected:</td><td><b>{xh_count}</b></td></tr>"
            f"<tr><td>❓ Unknown/other IDs:</td><td><b>{unknown}</b> — will skip</td></tr>"
            f"<tr><td>❌ Previously failed:</td><td><b>{prev_fail}</b> — will skip</td></tr>"
            f"<tr><td>&nbsp;</td><td></td></tr>"
            f"<tr><td><b>📥 To fetch:</b></td><td><b>{to_fetch}</b></td></tr>"
            f"</table><br>"
        )

        if to_fetch == 0:
            msg += "Nothing to do — all videos already have metadata."
            QMessageBox.information(self, "Pre-scan Complete", msg)
            return

        # Estimate time
        delay = self.settings.get("delay", 2)
        est_mins = (to_fetch * (delay + 3)) // 60
        msg += f"Estimated time: ~{est_mins} minutes<br><br>Start fetching now?"

        dlg = QMessageBox(self)
        dlg.setWindowTitle("Pre-scan Complete")
        dlg.setTextFormat(Qt.TextFormat.RichText)
        dlg.setText(msg)
        dlg.setStandardButtons(
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No)
        dlg.setDefaultButton(QMessageBox.StandardButton.Yes)
        r = dlg.exec()

        if r == QMessageBox.StandardButton.Yes:
            self._start_fetch_with_results(counts)

    # ── Fetch ─────────────────────────────────────────────────────────────────

    # ── Fetch control ────────────────────────────────────────────────────────

    # Start fetch from UI button — re-runs prescan first if no results cached.
    # Start fetching metadata. Creates WorkerSignals + FetchWorker,
    # connects all signals, and launches the worker thread.
    def _start_fetch(self):
        """Start fetch — run pre-scan first if we don't have results."""
        if self.worker and self.worker.isRunning():
            QMessageBox.information(self, "Already Running",
                "A fetch is already in progress.")
            return

        if self.scan_results:
            # Use existing scan results
            self._start_fetch_with_results(self.scan_results)
        else:
            # Need to scan first
            r = QMessageBox.question(self, "Pre-scan Required",
                "Run a pre-scan first to see what needs fetching?\n\n"
                "Click Yes to pre-scan, or No to start immediately without scanning.")
            if r == QMessageBox.StandardButton.Yes:
                self._do_prescan()
            else:
                # Build file list on the fly
                self._do_prescan()


    # Create FetchWorker with the scanned file list and start it.
    # Start fetch using pre-scan results. Sets up progress bar range
    # and begins processing the file list from the scan.
    def _start_fetch_with_results(self, counts):
        files = counts.get('files', [])
        if not files:
            QMessageBox.information(self, "Nothing to Fetch",
                "No files to fetch metadata for.")
            return

        total = len(files)
        self._log("info", f"Starting fetch — {total} files to process")

        # Reset counters
        self.lbl_fetched.setText("✅  Fetched: 0")
        self.lbl_skipped.setText("⏭  Skipped: 0")
        self.lbl_failed.setText("❌  Failed: 0")
        self.lbl_remaining.setText(f"⏳  Remaining: {total}")
        self.lbl_total.setText(f"Total: {total}")
        # Set range using 10000 steps per file for smooth sub-file movement
        self.progress_bar.setRange(0, total * 10000)
        self.progress_bar.setValue(0)
        self.scan_results = None  # clear so next run rescans

        signals = WorkerSignals()
        signals.progress.connect(self._on_progress, Qt.ConnectionType.QueuedConnection)
        signals.smooth_progress.connect(self._on_smooth_progress, Qt.ConnectionType.QueuedConnection)
        signals.completed.connect(self._on_completed, Qt.ConnectionType.QueuedConnection)
        signals.counters.connect(self._on_counters, Qt.ConnectionType.QueuedConnection)
        signals.log.connect(self._log, Qt.ConnectionType.QueuedConnection)
        signals.finished.connect(self._fetch_finished, Qt.ConnectionType.QueuedConnection)

        self.worker = FetchWorker(files, self.settings, signals)
        self.worker.start()

        # Fix 5: Disable buttons during fetch
        self.btn_scan.setEnabled(False)
        self.btn_start.setEnabled(False)
        self._start_shimmer()
        self.header_status.setText("● Fetching")
        self.header_status.setStyleSheet(
            "color: #4A90D9; font-size: 9px; font-family: monospace; "
            "background: transparent;")
        self.status_lbl.setText("Fetching metadata…")
        self.tray.setToolTip(f"{APP_NAME} — Fetching {total} files…")

    @pyqtSlot(str, str, int, int)

    # ── Worker signal handlers ────────────────────────────────────────────────

    # Update progress bar and current file label.
    @pyqtSlot(float, int)
    # Update the main progress bar smoothly using fractional progress.
    # Uses 10000 steps per file for sub-percent resolution.
    def _on_smooth_progress(self, fractional, total):
        """Update main progress bar smoothly."""
        if total > 0:
            new_val = int(fractional * 10000)
            if new_val != self.progress_bar.value():
                self.progress_bar.setValue(new_val)

    # Handle per-file progress signal — update current file label.
    def _on_progress(self, site, filename, current, total):
        short = filename[:70] + "…" if len(filename) > 70 else filename
        self.lbl_current.setText(
            f"[{current}/{total}]  {site.upper()}  {short}")
        # Ensure range matches total in case it changed
        # Progress bar handled by _on_smooth_progress

    @pyqtSlot(str, str, str)

    # Log a completed file entry (fetched/skipped/failed) to activity log.
    # Handle file completion signal — append to activity log with
    # colour-coded result (green=fetched, red=failed, grey=skipped).
    def _on_completed(self, result, site, filename):
        ts = datetime.now().strftime("%H:%M:%S")
        if result == "fetched" and self.chk_show_fetched.isChecked():
            self._append_log(f"[{ts}]  ✅  {site.upper()}  {filename}")
        elif result == "skipped" and self.chk_show_skipped.isChecked():
            self._append_log(f"[{ts}]  ⏭  {site.upper()}  {filename}")
        elif result == "failed" and self.chk_show_failed.isChecked():
            self._append_log(f"[{ts}]  ❌  {site.upper()}  {filename}")

    @pyqtSlot(int, int, int, int)

    # Update counter labels: fetched, skipped, failed, remaining.
    # Update the counter labels (Fetched, Skipped, Failed, Remaining).
    def _on_counters(self, fetched, skipped, failed, remaining):
        self.lbl_fetched.setText(f"✅  Fetched: {fetched}")
        self.lbl_skipped.setText(f"⏭  Skipped: {skipped}")
        self.lbl_failed.setText(f"❌  Failed: {failed}")
        self.lbl_remaining.setText(f"⏳  Remaining: {remaining}")
        self.tray.setToolTip(
            f"{APP_NAME} — ✅{fetched} ❌{failed} ⏳{remaining}")
        self.header_status.setText(
            f"● ✅{fetched} ❌{failed} ⏳{remaining}")

    @pyqtSlot()

    # Called when FetchWorker completes naturally.
    # Stops shimmer, shows completion dialog, cleans up worker.
    # Called when the FetchWorker thread completes. Re-enables UI,
    # shows completion notification, and updates failed ID count display.
    def _fetch_finished(self):
        self._set_ready_status()
        self._stop_shimmer()
        # Fix 5: Re-enable buttons
        self.btn_scan.setEnabled(True)
        self.btn_start.setEnabled(True)
        self.lbl_current.setText("Fetch complete")
        self.progress_bar.setValue(self.progress_bar.maximum())
        self.tray.setToolTip(APP_NAME)
        self.tray.showMessage(
            APP_NAME, "Metadata fetch complete!",
            QSystemTrayIcon.MessageIcon.Information, 4000)
        # Completion dialog
        fetched = self.worker.fetched if self.worker else 0
        skipped = self.worker.skipped if self.worker else 0
        failed  = self.worker.failed  if self.worker else 0
        msg = QMessageBox(self)
        msg.setWindowTitle("Fetch Complete")
        msg.setIcon(QMessageBox.Icon.Information)
        msg.setText(
            f"✅  Metadata fetch complete!\n\n"
            f"Fetched:  {fetched}\n"
            f"Skipped:  {skipped}\n"
            f"Failed:   {failed}")
        msg.setStandardButtons(QMessageBox.StandardButton.Ok)
        msg.exec()


    # Toggle pause state. Worker spin-waits on self._pause flag.
    # Toggle pause/resume. Updates button text and calls worker method.
    def _pause_fetch(self):
        if self.worker and self.worker.isRunning():
            if self.worker._pause:
                self.worker.resume()
                self.btn_pause.setText("⏸  Pause")
                self.status_lbl.setText("Fetching metadata…")
                self._start_shimmer()
            else:
                self.worker.pause()
                self.btn_pause.setText("▶  Resume")
                self.status_lbl.setText("Paused")
                self._stop_shimmer()


    # Set stop flag, null worker pointer, reset UI to ready state.
    # Stop fetching with confirmation dialog. Progress so far is kept.
    def _stop_fetch(self):
        if self.worker and self.worker.isRunning():
            r = QMessageBox.question(self, "Stop Fetch",
                "Stop fetching metadata?")
            if r == QMessageBox.StandardButton.Yes:
                self.worker.stop()
                self.status_lbl.setText("Stopping…")
                self._log("info", "Fetch stopped by user")

    def _set_ready_status(self):
        self.status_lbl.setText("Ready")
        # Fix 6: Always reset pause button and worker pause state
        self.btn_pause.setText("⏸  Pause")
        if self.worker:
            self.worker._pause = False
        self.header_status.setText("● Ready")
        self.header_status.setStyleSheet(
            "color: #00BFA5; font-size: 9px; font-family: monospace; "
            "background: transparent;")

    # ── Settings ──────────────────────────────────────────────────────────────
    def _open_settings(self):
        self.tabs.setCurrentIndex(1)

    def _add_folder(self):
        d = QFileDialog.getExistingDirectory(
            self, "Select Video Folder", os.path.expanduser("~/"))
        if d:
            self.folders_list.addItem(d)

    def _remove_folder(self):
        for item in self.folders_list.selectedItems():
            self.folders_list.takeItem(self.folders_list.row(item))


    # ── Settings ─────────────────────────────────────────────────────────────

    # Persist all settings tab values to QSettings INI file.
    # Save all settings tab values to the JSON config file.
    def _save_settings(self):
        delay_vals = [1, 2, 5]
        self.settings["scan_folders"] = [
            self.folders_list.item(i).text()
            for i in range(self.folders_list.count())
        ]
        self.settings["skip_existing"]   = self.chk_skip.isChecked()
        self.settings["delay"]           = delay_vals[self.set_delay.currentIndex()]
        self.settings["ytdlp_path"]      = self.set_ytdlp.text()
        cb_map = {0: "ask", 1: "tray", 2: "exit"}
        self.settings["close_behaviour"] = cb_map[self.set_close.currentIndex()]
        save_settings(self.settings)
        self.close_behaviour = self.settings["close_behaviour"]
        # Sync delay combo on fetch tab
        delay_map = {1: 0, 2: 1, 5: 2}
        self.delay_combo.setCurrentIndex(
            delay_map.get(self.settings["delay"], 1))
        QMessageBox.information(self, "Settings Saved", "Settings saved.")
        self._log("info", "Settings saved")


    # Live delay change — updates self.settings['delay_seconds'] immediately.
    # FetchWorker reads this value each iteration so change takes effect
    # after the current file completes without restarting.
    def _delay_changed(self, idx):
        delay_vals = [1, 2, 5]
        new_delay = delay_vals[idx]
        self.settings["delay"] = new_delay
        save_settings(self.settings)
        # Sync settings tab combo
        self.set_delay.setCurrentIndex(idx)
        # Confirm change in log if fetch is running
        if self.worker and self.worker.isRunning():
            self._log("info",
                f"Delay changed to {new_delay}s — takes effect after current file")

    # ── Logging ───────────────────────────────────────────────────────────────

    # ── Logging ──────────────────────────────────────────────────────────────

    # Format and append a log entry, update status bar.
    # Clear the cached list of failed video IDs so they will be
    # retried on the next fetch run. Shows confirmation dialog.
    def _clear_failed_list(self):
        """Clear the cached list of failed video IDs."""
        count = len(load_failed_ids())
        if count == 0:
            QMessageBox.information(self, "Failed List Empty",
                "There are no cached failed video IDs to clear.")
            return
        r = QMessageBox.question(self, "Clear Failed List",
            f"Clear {count} cached failed video ID(s)?\n\n"
            "These videos will be retried on the next fetch run.",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.Cancel,
            QMessageBox.StandardButton.Cancel)
        if r == QMessageBox.StandardButton.Yes:
            clear_failed_ids()
            self.lbl_failed_count.setText(
                "MetaFetch remembers videos that failed to fetch (deleted from platform).\n"
                "These are automatically skipped on future runs.\n\n"
                "Currently cached: 0 failed video ID(s)")
            self._log("info", f"Cleared {count} cached failed video IDs")

    # Custom right-click context menu for the activity log.
    # Options: Copy Line, Copy Selection, Select All.
    def _log_context_menu(self, pos):
        """Custom right-click context menu for the activity log."""
        menu = QMenu(self)

        # Get the line under the cursor
        cursor = self.log_view.cursorForPosition(pos)
        cursor.select(cursor.SelectionType.LineUnderCursor)
        line = cursor.selectedText().strip()

        # Copy line
        act_copy = menu.addAction("📋  Copy Line")
        act_copy.setEnabled(bool(line))

        # Copy selected text
        has_selection = self.log_view.textCursor().hasSelection()
        act_copy_sel = menu.addAction("📋  Copy Selection")
        act_copy_sel.setEnabled(has_selection)

        menu.addSeparator()

        # Select all
        act_select_all = menu.addAction("📄  Select All")

        # Execute menu
        action = menu.exec(self.log_view.mapToGlobal(pos))

        if action == act_copy:
            QApplication.clipboard().setText(line)
        elif action == act_copy_sel:
            self.log_view.copy()
        elif action == act_select_all:
            self.log_view.selectAll()

    def _log(self, level, message):
        ts = datetime.now().strftime("%H:%M:%S")
        icons = {"info": "ℹ", "warn": "⚠", "error": "✖"}
        icon = icons.get(level, "•")
        self._append_log(f"[{ts}] {icon}  {message}")
        self.status_lbl.setText(message[:80])

    def _append_log(self, text):
        self.log_view.append(text)
        self.log_view.moveCursor(QTextCursor.MoveOperation.End)

    # ── Dependency check ──────────────────────────────────────────────────────

    # ── Startup checks ───────────────────────────────────────────────────────

    # Verify yt-dlp is installed and accessible. Warn if not found.
    # Verify yt-dlp is installed and accessible on startup.
    # Shows a warning dialog with install instructions if not found.
    def _check_dependencies(self):
        ytdlp = self.settings.get("ytdlp_path", "yt-dlp")
        try:
            co_kwargs = dict(text=True, stderr=subprocess.DEVNULL)
            if sys.platform == "win32":
                co_kwargs["creationflags"] = subprocess.CREATE_NO_WINDOW
            ver = subprocess.check_output(
                [get_bin(ytdlp), "--version"], **co_kwargs).strip()
            self._log("info", f"yt-dlp found: {ver}")
        except Exception:
            dlg = QDialog(self)
            dlg.setWindowTitle("yt-dlp Not Found")
            dlg.setMinimumWidth(420)
            layout = QVBoxLayout(dlg)
            layout.setContentsMargins(20, 20, 20, 20)
            layout.addWidget(QLabel(
                "<b>❌  yt-dlp is not installed or not found.</b><br><br>"
                "MetaFetch requires yt-dlp to fetch metadata.<br>"
                "Install it with:",
                textFormat=Qt.TextFormat.RichText))
            code = QTextEdit()
            code.setReadOnly(True)
            code.setMaximumHeight(60)
            code.setFont(QFont("monospace", 9))
            code.setPlainText("sudo apt update\nsudo apt install yt-dlp")
            layout.addWidget(code)
            layout.addWidget(QLabel("After installing, restart MetaFetch."))
            btn = QPushButton("OK")
            btn.clicked.connect(dlg.accept)
            layout.addWidget(btn)
            dlg.exec()
            self.header_status.setText("⚠ yt-dlp missing")
            self.header_status.setStyleSheet(
                "color: #cc0000; font-size: 9px; font-family: monospace; "
                "background: transparent;")

    # ── Help ──────────────────────────────────────────────────────────────────
    # Show the in-app help dialog with sidebar topic navigation.
    # Contains: Getting Started, Adding Folders, Pre-scan, Fetching,
    # Settings, After MetaFetch, Troubleshooting, and FAQ sections.
    def _open_help(self):
        dlg = QDialog(self)
        dlg.setWindowTitle(f"{APP_NAME} — Help")
        dlg.resize(780, 520)
        outer = QVBoxLayout(dlg)
        layout = QHBoxLayout()

        sidebar = QListWidget()
        sidebar.setMaximumWidth(170)
        topics = [
            "Getting Started",
            "How It Works",
            "Pre-scan",
            "Running a Fetch",
            "Settings",
            "Delay — Why It Matters",
            "PH IDs",
            "XH IDs",
            "Using with Stash",
            "Troubleshooting",
            "Keyboard Shortcuts",
        ]
        for t in topics:
            sidebar.addItem(t)
        layout.addWidget(sidebar)

        content = QTextEdit()
        content.setReadOnly(True)
        content.setFont(QFont("sans-serif", 9))
        layout.addWidget(content)

        help_text = {
            "Getting Started": """
<h2>Getting Started</h2>
<p>MetaFetch automatically fetches <b>.info.json</b> metadata files for your locally
downloaded videos identified by PH or XH ID patterns using yt-dlp.</p>
<h3>Quick setup</h3>
<ol>
<li>Install yt-dlp: <code>sudo apt install yt-dlp</code></li>
<li>Go to <b>Settings</b> tab and add your video folders</li>
<li>Click <b>Save Settings</b></li>
<li>Click <b>Pre-scan Folders</b> to see what needs fetching</li>
<li>Click <b>Start Fetch</b></li>
</ol>
""",
            "How It Works": """
<h2>How It Works</h2>
<p>For each .mp4 file in your configured folders:</p>
<ol>
<li>Checks if a <b>.info.json</b> file already exists next to the video — if so, skips it</li>
<li>Extracts the <b>video ID</b> from the filename (the part in square brackets)</li>
<li>Determines whether it's a PH or XH ID from the format</li>
<li>Calls yt-dlp with <b>--skip-download --write-info-json</b> to fetch just the metadata</li>
<li>Saves the .info.json file next to the video with the same base filename</li>
</ol>
<p>Your video files are <b>never modified, moved, or deleted</b>.</p>
""",
            "Pre-scan": """
<h2>Pre-scan</h2>
<p>Click <b>Pre-scan Folders</b> before starting a fetch to see exactly what will happen:</p>
<ul>
<li>Total .mp4 files found</li>
<li>How many already have .info.json (will be skipped)</li>
<li>How many PH and XH IDs were detected</li>
<li>How many unknown IDs will be skipped</li>
<li>Total to fetch and estimated time</li>
</ul>
<p>After the scan you can choose to start immediately or cancel.</p>
""",
            "Running a Fetch": """
<h2>Running a Fetch</h2>
<p>Click <b>Start Fetch</b> to begin. A pre-scan runs first if one hasn't been done.</p>
<p>The activity log shows each file as it's processed:</p>
<ul>
<li><b>✅</b> — metadata fetched successfully</li>
<li><b>⏭</b> — already had .info.json, skipped</li>
<li><b>❌</b> — failed (video deleted, private, or rate-limited)</li>
</ul>
<p>You can hide skipped entries from the log using the checkboxes — skipped
entries can be very numerous and noisy for large libraries.</p>
<p>Use <b>Pause</b> to temporarily halt and <b>Resume</b> to continue.
Use <b>Stop</b> to cancel — safe to restart, already-fetched files won't be re-fetched.</p>
""",
            "Settings": """
<h2>Settings</h2>
<h3>Video Folders</h3>
<p>Add all folders where your downloaded videos are stored.
MetaFetch will scan each folder for .mp4 files.</p>
<h3>Skip Existing</h3>
<p>When checked (recommended), videos that already have a .info.json file are skipped.
This makes it safe to re-run after adding new videos.</p>
<h3>Request Delay</h3>
<p>How long to wait between requests to the video site.
Faster is quicker but risks rate-limiting. Normal (2s) is recommended.</p>
<h3>yt-dlp Path</h3>
<p>Usually just <code>yt-dlp</code> if installed system-wide.
Change this if yt-dlp is installed in a non-standard location.</p>
""",
            "Delay — Why It Matters": """
<h2>Delay — Why It Matters</h2>
<p>Between each metadata request, MetaFetch waits a short time.
This is important for two reasons:</p>
<h3>Politeness to the server</h3>
<p>Sending hundreds of requests per minute to a website is considered
abusive behaviour and can get your IP address temporarily blocked.</p>
<h3>Rate limiting</h3>
<p>The source platforms both rate-limit excessive traffic.
If you see a high number of failures, increase the delay to 5 seconds.</p>
<h3>Time estimate</h3>
<p>With a 2-second delay plus ~3 seconds per request, expect roughly
3-5 seconds per video. For 4,000 videos that's approximately 4-6 hours.
Run it overnight and come back to a completed library.</p>
""",
            "PH IDs": """
<h2>PH Video IDs</h2>
<p>Videos with PH-pattern IDs (downloaded with yt-dlp) come in one of two formats:</p>
<ul>
<li><b>ph-prefixed</b>: <code>ph63841ed468601</code>, <code>ph5baafe340bca3</code></li>
<li><b>Plain hex</b>: <code>659876e67fcb5</code>, <code>66d8ad61bc973</code> (10+ hex chars)</li>
</ul>
<p>Both formats use the same URL structure and are handled automatically.</p>
<p>Short purely-numeric IDs (less than 10 digits) are from other sites and are skipped.</p>
""",
            "XH IDs": """
<h2>XH Video IDs</h2>
<p>Videos with XH-pattern IDs (downloaded with yt-dlp) start with <b>xh</b>
followed by alphanumeric characters:</p>
<ul>
<li><code>xhTJwau</code></li>
<li><code>xhRTMF9</code></li>
<li><code>xhf0UaK</code></li>
</ul>
<p>The script builds the XH URL by combining the title slug
(derived from the filename) with the ID.</p>
""",
            "Using with Stash": """
<h2>Using .info.json Files with Stash</h2>
<p>Once MetaFetch has run and your .info.json files are in place,
tell Stash to read them:</p>
<ol>
<li>In Stash go to: <b>Settings → Metadata Providers → Scrapers</b></li>
<li>Find and install a <b>LocalFile or JSON scraper</b> from Community scrapers</li>
<li>Go to <b>Settings → Tasks → Identify</b></li>
<li>Add the JSON/LocalFile scraper as a source</li>
<li>Run the Identify task</li>
</ol>
<p>Stash reads the .info.json files directly — no internet connection needed,
works even for videos deleted from their source site.</p>
<h3>Tip: fetch at download time</h3>
<p>Add <code>--write-info-json</code> to your yt-dlp config
at <code>~/.config/yt-dlp/config</code> so future downloads always
include metadata automatically.</p>
""",
            "Troubleshooting": """
<h2>Troubleshooting</h2>
<h3>High failure rate</h3>
<p>Videos get deleted from sites over time — 10-20% failure is normal for older libraries.
Above 50% suggests rate-limiting — increase delay to 5 seconds and try again later.</p>
<h3>No IDs found in filenames</h3>
<p>The video was not downloaded with yt-dlp or was renamed after downloading.
MetaFetch requires the video ID in square brackets at the end of the filename.</p>
<h3>yt-dlp not found</h3>
<p>Install it: <code>sudo apt install yt-dlp</code><br>
Or check the path in Settings.</p>
<h3>Nothing fetched — 0 in counter</h3>
<p>Check your folder paths in Settings. Confirm they exist and contain .mp4 files.</p>
""",
            "Keyboard Shortcuts": """
<h2>Keyboard Shortcuts</h2>
<table>
<tr><td><b>Ctrl+P</b></td><td>Pre-scan folders</td></tr>
<tr><td><b>Ctrl+Enter</b></td><td>Start fetch</td></tr>
<tr><td><b>Space</b></td><td>Pause / Resume</td></tr>
<tr><td><b>Ctrl+.</b></td><td>Stop fetch</td></tr>
<tr><td><b>Ctrl+,</b></td><td>Settings</td></tr>
<tr><td><b>F1</b></td><td>Help</td></tr>
<tr><td><b>Ctrl+Q</b></td><td>Quit</td></tr>
</table>
""",
        }

        def show_topic(item):
            content.setHtml(help_text.get(item.text(), "<p>No content.</p>"))

        sidebar.currentItemChanged.connect(
            lambda cur, prev: show_topic(cur) if cur else None)
        sidebar.setCurrentRow(0)
        show_topic(sidebar.item(0))

        outer.addLayout(layout)
        btn_close = QPushButton("Close")
        btn_close.clicked.connect(dlg.close)
        btn_row = QHBoxLayout()
        btn_row.addStretch()
        btn_row.addWidget(btn_close)
        outer.addLayout(btn_row)
        dlg.exec()

    def _show_shortcuts(self):
        QMessageBox.information(self, "Keyboard Shortcuts", """
Ctrl+P        Pre-scan folders
Ctrl+Enter    Start fetch
Space         Pause / Resume
Ctrl+.        Stop fetch
Ctrl+,        Settings
F1            Help
Ctrl+Q        Quit
""")

    # Show the About dialog with app icon, version, yt-dlp version,
    # copyright, and contact email.
    def _show_about(self):
        dlg = QDialog(self)
        dlg.setWindowTitle(f"About {APP_NAME}")
        dlg.setFixedSize(360, 260)
        layout = QVBoxLayout(dlg)
        layout.setContentsMargins(20, 20, 20, 20)
        layout.setSpacing(8)

        if os.path.exists(ICON_PATH):
            lbl_icon = QLabel()
            pix = QPixmap(ICON_PATH).scaled(
                64, 64,
                Qt.AspectRatioMode.KeepAspectRatio,
                Qt.TransformationMode.SmoothTransformation)
            lbl_icon.setPixmap(pix)
            lbl_icon.setAlignment(Qt.AlignmentFlag.AlignCenter)
            layout.addWidget(lbl_icon)

        for text, bold in [
            (APP_NAME, True), (f"Version {APP_VERSION}", False),
            (APP_COPY, False), (APP_EMAIL, False)
        ]:
            lbl = QLabel(text)
            if bold:
                lbl.setFont(QFont("sans-serif", 13, QFont.Weight.Bold))
            lbl.setAlignment(Qt.AlignmentFlag.AlignCenter)
            layout.addWidget(lbl)

        layout.addStretch()
        try:
            co_kwargs = dict(text=True, stderr=subprocess.DEVNULL)
            if sys.platform == "win32":
                co_kwargs["creationflags"] = subprocess.CREATE_NO_WINDOW
            ytdlp_ver = subprocess.check_output(
                [self.settings.get("ytdlp_path", "yt-dlp"), "--version"],
                **co_kwargs).strip()
        except Exception:
            ytdlp_ver = "not found"
        import PyQt6.QtCore
        lbl_info = QLabel(
            f"yt-dlp: {ytdlp_ver}  |  Python: {sys.version.split()[0]}  |  "
            f"PyQt6: {PyQt6.QtCore.PYQT_VERSION_STR}")
        lbl_info.setFont(QFont("monospace", 7))
        lbl_info.setAlignment(Qt.AlignmentFlag.AlignCenter)
        layout.addWidget(lbl_info)

        btn = QPushButton("Close")
        btn.clicked.connect(dlg.close)
        layout.addWidget(btn)
        dlg.exec()

    # ── Misc ──────────────────────────────────────────────────────────────────
    def _view_owned_log(self):
        """View the already-owned log file."""
        log_file = os.path.expanduser("~/.config/metafetch/already_owned.log")
        if not os.path.exists(log_file):
            QMessageBox.information(self, "No Log",
                "No already-owned log file found yet.")
            return
        dlg = QDialog(self)
        dlg.setWindowTitle("Already-Owned Log")
        dlg.resize(700, 400)
        layout = QVBoxLayout(dlg)
        text = QTextEdit()
        text.setReadOnly(True)
        text.setFont(QFont("monospace", 8))
        with open(log_file) as f:
            content_log = f.read()
        text.setPlainText(content_log if content_log else "(empty)")
        layout.addWidget(text)
        btn_row = QHBoxLayout()
        btn_close = QPushButton("Close")
        btn_close.clicked.connect(dlg.close)
        btn_row.addStretch()
        btn_row.addWidget(btn_close)
        layout.addLayout(btn_row)
        dlg.exec()

    def _clear_owned_log(self):
        """Clear the already-owned log file."""
        log_file = os.path.expanduser("~/.config/metafetch/already_owned.log")
        r = QMessageBox.question(self, "Clear Log",
            "Clear the already-owned log?\n\n"
            "This does not affect your video files or metadata.")
        if r == QMessageBox.StandardButton.Yes:
            try:
                open(log_file, "w").close()
                self._log("info", "Already-owned log cleared")
            except Exception as e:
                QMessageBox.warning(self, "Error", f"Could not clear log: {e}")


    # ── Reset ────────────────────────────────────────────────────────────────

    # Reset all counters, progress bar and log. Only when not fetching.
    # Reset all counters, progress bar, and activity log back to
    # their initial state.
    def _reset_display(self):
        """Reset all counters and clear the activity log."""
        if self.worker and self.worker.isRunning():
            QMessageBox.warning(self, "Fetch Running",
                "Cannot reset while a fetch is in progress.")
            return
        self._stop_shimmer()
        self.lbl_fetched.setText("✅  Fetched: 0")
        self.lbl_skipped.setText("⏭  Skipped: 0")
        self.lbl_failed.setText("❌  Failed: 0")
        self.lbl_remaining.setText("⏳  Remaining: 0")
        self.lbl_total.setText("Total: 0")
        self.progress_bar.setValue(0)
        self.lbl_current.setText("No fetch in progress")
        self.log_view.clear()
        self._set_ready_status()
        self._log("info", "Display reset — ready for new session")

    def _shimmer_tick(self):
        """Wide soft glow sweeping left to right — long leading and trailing fade."""
        self._shimmer_pos = (self._shimmer_pos + 1) % 300

        bar = self.progress_bar
        mn, mx = bar.minimum(), bar.maximum()
        val = bar.value()
        fill = (val - mn) / (mx - mn) if mx > mn and val > mn else 0.0

        if fill <= 0:
            return

        beam_width = 1.0
        lead  = (self._shimmer_pos / 299.0) * 2.0 - 1.0
        trail = lead + beam_width

        vis_left  = max(0.0, lead)
        vis_right = min(1.0, trail)

        if vis_right <= vis_left:
            return

        peak = (vis_left + vis_right) / 2.0

        s0 = 0.0
        s1 = max(0.001, min(vis_left, peak - 0.01))
        s2 = max(s1 + 0.001, min(peak, 0.999))
        s3 = max(s2 + 0.001, min(vis_right, 0.999))
        s4 = 1.0

        stops = (f"stop:{s0:.3f} #2255aa, "
                 f"stop:{s1:.3f} #2255aa, "
                 f"stop:{s2:.3f} #7ab8f0, "
                 f"stop:{s3:.3f} #2255aa, "
                 f"stop:{s4:.3f} #2255aa")

        shimmer_style = f"""
            QProgressBar {{
                border: 1px solid #cccccc;
                border-radius: 4px;
                background: #ffffff;
                max-height: 10px;
            }}
            QProgressBar::chunk {{
                border-radius: 4px;
                background: qlineargradient(
                    x1:0, y1:0, x2:1, y2:0,
                    {stops}
                );
            }}
        """
        self.progress_bar.setStyleSheet(shimmer_style)

    def _start_shimmer(self):
        """Start the shimmer animation."""
        self._shimmer_pos = 0
        self._shimmer_timer.start(30)

    def _stop_shimmer(self):
        """Stop shimmer and reset to default style."""
        self._shimmer_timer.stop()
        self.progress_bar.setStyleSheet("""
            QProgressBar {
                border: 1px solid #cccccc;
                border-radius: 4px;
                background: #ffffff;
                max-height: 10px;
            }
            QProgressBar::chunk {
                border-radius: 4px;
                background: #4A90D9;
            }
        """)

    def _open_first_folder(self):
        folders = self.settings.get("scan_folders", [])
        if folders and os.path.exists(folders[0]):
            (lambda p: __import__("os").startfile(p) if __import__("sys").platform == "win32" else __import__("subprocess").Popen(["xdg-open", p]))(folders[0])

    def _toggle_window(self):
        if self.isVisible():
            self.hide()
        else:
            self.show()
            self.raise_()
            self.activateWindow()

    def _tray_activated(self, reason):
        if reason == QSystemTrayIcon.ActivationReason.DoubleClick:
            self._toggle_window()

    # Quit the application. Warns if a fetch is in progress.
    def _quit(self):
        if self.worker and self.worker.isRunning():
            r = QMessageBox.question(
                self, "Fetch In Progress",
                "A fetch is in progress. Exit anyway?")
            if r != QMessageBox.StandardButton.Yes:
                return
        QApplication.quit()

    # Handle window close button. Behaviour depends on settings:
    # ask (show dialog), tray (minimise to tray), or exit (quit).
    def closeEvent(self, event):
        behaviour = self.close_behaviour
        if behaviour == "ask":
            dlg = QDialog(self)
            dlg.setWindowTitle(f"Close {APP_NAME}")
            layout = QVBoxLayout(dlg)
            layout.addWidget(QLabel("What would you like to do?"))
            chk = QCheckBox("Remember this choice")
            btn_tray = QPushButton("Minimise to System Tray")
            btn_exit = QPushButton("Exit Completely")
            btn_cancel = QPushButton("Cancel")
            layout.addWidget(btn_tray)
            layout.addWidget(btn_exit)
            layout.addWidget(btn_cancel)
            layout.addWidget(chk)
            choice = [None]
            btn_tray.clicked.connect(lambda: [choice.__setitem__(0,"tray"), dlg.accept()])
            btn_exit.clicked.connect(lambda: [choice.__setitem__(0,"exit"), dlg.accept()])
            btn_cancel.clicked.connect(dlg.reject)
            dlg.exec()
            if choice[0] is None:
                event.ignore()
                return
            if chk.isChecked():
                self.close_behaviour = choice[0]
                self.settings["close_behaviour"] = choice[0]
                save_settings(self.settings)
            behaviour = choice[0]

        if behaviour == "tray":
            event.ignore()
            self.hide()
            self.tray.showMessage(
                APP_NAME, "Running in background — right-click tray icon",
                QSystemTrayIcon.MessageIcon.Information, 2000)
        else:
            self._quit()
            event.accept()


# ── Exception hook ────────────────────────────────────────────────────────────
# ── Entry point ─────────────────────────────────────────────────────────────

# Global exception handler — shows a dialog on unhandled exceptions
# instead of silently crashing the GUI.
def exception_hook(exc_type, exc_value, exc_tb):
    import traceback
    tb = "".join(traceback.format_exception(exc_type, exc_value, exc_tb))
    print(tb)
    try:
        msg = QMessageBox()
        msg.setWindowTitle("Unexpected Error")
        msg.setIcon(QMessageBox.Icon.Critical)
        msg.setText("An unexpected error occurred.")
        msg.setDetailedText(tb)
        msg.exec()
    except Exception:
        pass


# ── Entry point ───────────────────────────────────────────────────────────────

# Prevent multiple instances of MetaFetch running simultaneously.
# Uses a PID lock file in the system temp directory.
def check_single_instance():
    """Prevent multiple instances running simultaneously."""
    import os, sys
    import tempfile
    lock_file = os.path.join(tempfile.gettempdir(), "metafetch.instance.lock")
    pid = os.getpid()

    # Check for existing lock
    if os.path.exists(lock_file):
        try:
            with open(lock_file) as f:
                existing_pid = int(f.read().strip())
            # Check if that PID is still running
            if sys.platform == "win32":
                import ctypes
                handle = ctypes.windll.kernel32.OpenProcess(0x100000, False, existing_pid)
                if handle:
                    ctypes.windll.kernel32.CloseHandle(handle)
                    raise ProcessLookupError
            else:
                os.kill(existing_pid, 0)
            # It is running — show error and exit
            app = QApplication.instance() or QApplication(sys.argv)
            QMessageBox.warning(
                None,
                f"MetaFetch — Already Running",
                f"⚠️  MetaFetch is already open.\n\n"
                f"Only one instance can run at a time.\n"
                f"Please switch to the existing window.",
                QMessageBox.StandardButton.Ok
            )
            sys.exit(0)
        except (ValueError, ProcessLookupError, PermissionError):
            # Stale lock — clean it up
            os.remove(lock_file)

    # Write our PID to the lock file
    with open(lock_file, 'w') as f:
        f.write(str(pid))

    # Remove lock on exit
    import atexit
    atexit.register(lambda: os.path.exists(lock_file) and os.remove(lock_file))

# Application entry point. Sets up single-instance check, exception
# hook, QApplication, and shows the main window.
def main():
    check_single_instance()
    sys.excepthook = exception_hook
    app = QApplication(sys.argv)
    app.setApplicationName(APP_NAME)
    app.setApplicationVersion(APP_VERSION)
    app.setOrganizationName(APP_AUTHOR)
    if os.path.exists(ICON_PATH):
        app.setWindowIcon(QIcon(ICON_PATH))
    window = MainWindow()
    window.show()
    sys.exit(app.exec())

if __name__ == "__main__":
    main()
