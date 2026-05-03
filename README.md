<p align="center">
  <img src="screenshots/mf_icon.png" alt="MetaFetch" width="128">
</p>

<h1 align="center">MetaFetch</h1>

<p align="center">
  <strong>Free desktop metadata fetcher for existing video libraries</strong><br>
  Powered by yt-dlp · Built with Python &amp; PyQt6
</p>

<p align="center">
  <a href="https://videofetcher.co.uk/metafetch.html">⬇️ Download</a> ·
  <a href="https://videofetcher.co.uk/plugins.html">🧩 Info JSON Importer Plugin</a> ·
  <a href="https://videofetcher.co.uk">🌐 Website</a>
</p>

---

## What Is MetaFetch?

MetaFetch scans your existing local video library and downloads missing `.info.json` metadata files using yt-dlp — **without re-downloading the videos themselves**.

Ideal if your library was built before metadata saving was enabled, or videos were downloaded with different tools.

---

## Features

### Smart Library Scanning
- Scans configured folders for `.mp4` files missing `.info.json` sidecars
- Extracts embedded video IDs from filenames
- Automatically identifies supported source platforms

### Pre-scan Analysis
- Total videos detected
- Files already containing metadata
- Supported IDs found
- Unknown files skipped
- Estimated fetch workload

### Metadata-Only Fetching
- Downloads `.info.json` only — never re-downloads videos
- Adjustable request delay (1s / 2s / 5s)
- Sequential rate-limited requests
- Live progress monitoring

### Intelligent Retry Protection
Failed or deleted video IDs are cached automatically to avoid repeated unnecessary retries.

### Desktop GUI Features
- Start / Pause / Resume / Stop
- Live activity log with right-click copy
- System tray support
- Persistent settings
- Built-in help system

---

## Screenshots

<p align="center">
  <img src="screenshots/MFdownloadactive.png" alt="Active Fetch" width="48%">
  <img src="screenshots/MFprescan.png" alt="Pre-scan" width="48%">
</p>
<p align="center">
  <img src="screenshots/MFsettings.png" alt="Settings" width="48%">
  <img src="screenshots/MFabout.png" alt="About" width="48%">
</p>

---

## How It Works

Before: My Video [VIDEO_ID].mp4
After:  My Video [VIDEO_ID].mp4 + My Video [VIDEO_ID].info.json

MetaFetch uses the ID embedded in the filename to fetch the correct metadata.

---

## Download

| Platform | Download |
|----------|----------|
| 🐧 **Linux x86_64** | [Download](https://videofetcher.co.uk/files/MetaFetch) |
| 🪟 **Windows x64** | [Download](https://videofetcher.co.uk/files/MetaFetch.exe) |

Standalone executable — no installation required.

**Linux:** wget https://videofetcher.co.uk/files/MetaFetch then chmod +x MetaFetch then ./MetaFetch

Requires: sudo apt install yt-dlp

**Windows:** Download MetaFetch.exe and double-click. No installation needed.

---

## Companion Apps

### Video Fetcher 2026
Batch video downloader with parallel slots, scraper and queue management.
👉 **[videofetcher.co.uk](https://videofetcher.co.uk)**

### Stash Info JSON Importer Plugin (Free)
Imports `.info.json` metadata into [Stash](https://stashapp.cc/) media manager.
👉 **[Download Plugin](https://videofetcher.co.uk/plugins.html)**

---

## Tech Stack

- **Language:** Python 3 / PyQt6
- **Metadata Engine:** yt-dlp
- **Compiler:** PyInstaller
- **Platforms:** Linux x86_64, Windows x64

---

## Links

- 🌐 **Website:** [videofetcher.co.uk](https://videofetcher.co.uk)
- ⬇️ **Download:** [videofetcher.co.uk/metafetch.html](https://videofetcher.co.uk/metafetch.html)
- 🧩 **Plugin:** [videofetcher.co.uk/plugins.html](https://videofetcher.co.uk/plugins.html)
- ☕ **Support:** [videofetcher.co.uk/support.html](https://videofetcher.co.uk/support.html)
- 📧 **Contact:** [david@videofetcher.co.uk](mailto:david@videofetcher.co.uk)

---

<p align="center">
  <strong>MetaFetch</strong><br>
  © 2026 David Smith · <a href="https://videofetcher.co.uk">videofetcher.co.uk</a><br>
  <a href="mailto:david@videofetcher.co.uk">david@videofetcher.co.uk</a>
</p>