"""
obsidian_to_zotero.py
Sync Kindle/Obsidian highlights to Zotero from iPad (Pyto).

- Uses Markdown from "Kindle Highlights" Obsidian plugin:
  https://github.com/hadynz/obsidian-kindle-plugin
- Strict duplicate prevention + item-key caching
- Robust key extraction from POST (no index lag issues)
- DRY_RUN safety (no monkeypatching; writes go through safe_* helpers)
- Resume support + per-title filtering + batch limiting

Setup:
1) Copy secrets.example.json → secrets.json with:
   {
     "ZOTERO_API_KEY": "<YOUR_ZOTERO_API_KEY>",
     "ZOTERO_USER_ID": "<YOUR_ZOTERO_USER_ID>",
     "USE_GROUP": false
   }
2) Put your exported notes in: On My iPad → Pyto → AmazonNotes
"""

import re
import sys
import json
import time
import string
import hashlib
from pathlib import Path
from typing import Optional, List

import requests
from requests import Session

# --- Run mode (simple + safe) ---
DRY_RUN = False   # ← set False to actually write to Zotero (you'll get a SEND NOW confirm)

# --- New convenience knobs ---
RESUME_ENABLED = True                 # skip books already completed in previous live runs
ONLY_TITLES: List[str] = []           # e.g. ["smart notes", "foundation"] (case-insensitive contains); [] = all
BATCH_LIMIT = 0                       # process at most N matching books; 0 = no limit

# ------------------- CONFIG -------------------
secrets_path = Path(__file__).parent / "secrets.json"
if not secrets_path.exists():
    raise RuntimeError("Missing secrets.json — copy secrets.example.json to secrets.json and fill in your values.")
secrets = json.loads(secrets_path.read_text(encoding="utf-8"))

ZOTERO_API_KEY: str = secrets["ZOTERO_API_KEY"]
ZOTERO_USER_ID: str = secrets["ZOTERO_USER_ID"]   # user id, or group id if USE_GROUP=True
USE_GROUP: bool = secrets.get("USE_GROUP", False)

COLLECTION_NAME = "Books"
OBSIDIAN_VAULT_PATH = str(Path.home() / "Documents" / "AmazonNotes")

API_BASE = f"https://api.zotero.org/{'groups' if USE_GROUP else 'users'}/{ZOTERO_USER_ID}"
HEADERS_JSON = {"Zotero-API-Key": ZOTERO_API_KEY, "Content-Type": "application/json"}
HEADERS = {"Zotero-API-Key": ZOTERO_API_KEY}

# --- Safe network helpers (no monkeypatching) ---
SESSION = Session()

class _FakeResp:
    status_code = 200
    text = "[DRY RUN] blocked network call"
    headers = {}
    def json(self): return {}

def safe_post(url, **kwargs):
    if DRY_RUN:
        print("[DRY RUN] BLOCKED POST →", url)
        return _FakeResp()
    return SESSION.post(url, **kwargs)

def safe_put(url, **kwargs):
    if DRY_RUN:
        print("[DRY RUN] BLOCKED PUT  →", url)
        return _FakeResp()
    return SESSION.put(url, **kwargs)

# ---------- LIVE confirmation ----------
def confirm_live_or_abort():
    try:
        import pyto_ui
        a = pyto_ui.Alert("Confirm LIVE run", "This will WRITE to Zotero.\nProceed?")
        a.add_action("SEND NOW")  # first button
        a.add_action("Cancel")
        result = a.show()
        s = (str(result) if result is not None else "").strip().lower()
        print(f"[DEBUG] Alert.show() -> {result!r} | normalized='{s}'")
        proceed = (result == 0) if isinstance(result, int) else (s in {"send now", "send"})
        if not proceed:
            print("LIVE run canceled. Exiting."); sys.exit(0)
    except Exception as e:
        print("Could not show confirmation UI (", e, "). Aborting LIVE run for safety."); sys.exit(0)

# ---------- Sent-log (prefer vault, fallback local) ----------
def sent_log_path(vault_path: str) -> Path:
    return Path(vault_path) / ".sent_highlights.json"

LOCAL_SENT_LOG = Path(".sent_highlights_local.json")  # fallback alongside script

def load_sent_log(vault_path: str) -> dict:
    data = {}
    try:
        p = sent_log_path(vault_path)
        if p.exists():
            data = json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        data = {}
    try:
        if LOCAL_SENT_LOG.exists():
            fallback = json.loads(LOCAL_SENT_LOG.read_text(encoding="utf-8"))
            data = {**fallback, **data}
    except Exception:
        pass
    data.setdefault("_items", {})        # normalized_title -> zotero_item_key
    data.setdefault("_done_titles", [])  # list of normalized titles processed to completion
    return data

def save_sent_log(vault_path: str, log: dict) -> None:
    vp = Path(vault_path)
    try:
        test = vp / ".pyto_write_test.tmp"
        test.write_text("ok", encoding="utf-8"); test.unlink(missing_ok=True)
        sent_log_path(vault_path).write_text(json.dumps(log, indent=2), encoding="utf-8"); return
    except Exception:
        pass
    LOCAL_SENT_LOG.write_text(json.dumps(log, indent=2), encoding="utf-8")
    print(f"⚠️ No write permission in vault; saved sent-log to {LOCAL_SENT_LOG.name} instead.")

# ---------- Normalization ----------
def normalize_title(s: str) -> str:
    if not s: return ""
    table = str.maketrans("", "", string.punctuation + "“”‘’")
    return " ".join(s.lower().translate(table).split())

def normalize_author(s: str) -> str:
    if not s: return ""
    return " ".join(s.lower().split())

def title_matches_filters(title: str) -> bool:
    if not ONLY_TITLES: return True
    t = title.lower()
    return any(substr.lower() in t for substr in ONLY_TITLES)

# ---------- Parser (Kindle→Obsidian export style) ----------
def parse_kindle_md(path: Path):
    txt = path.read_text(encoding="utf-8")
    m_title = re.search(r"^#\s*(.+)", txt, re.MULTILINE)
    title = m_title.group(1).strip() if m_title else None
    m_author = re.search(r"Author:\s*\[?([^\]\n]+)\]?", txt, re.IGNORECASE)
    author = m_author.group(1).strip() if m_author else None
    blocks = [b.strip() for b in txt.split("\n---\n") if b.strip()]
    highs = []
    for b in blocks:
        if b.startswith("#"): continue
        if "location" not in b.lower(): continue
        loc_m = re.search(r"location[:\s]+([0-9,-]+)", b, re.IGNORECASE)
        loc = loc_m.group(1) if loc_m else None
        note_m = re.search(r"(?:Note|My Note)[:\s]+(.+)$", b, re.IGNORECASE | re.DOTALL)
        note = note_m.group(1).strip() if note_m else None
        first_line = b.split("\n", 1)[0]
        text = re.sub(r"\s*[—-]\s*location:.*$", "", first_line, flags=re.IGNORECASE).strip()
        if text:
            highs.append({"text": text, "location": loc, "note": note})
    return title, author, highs

# ---------- Zotero: collections ----------
def get_or_create_collection(name: str, dry_run: bool) -> Optional[str]:
    r = requests.get(f"{API_BASE}/collections", headers=HEADERS, params={"q": name, "limit": 100}, timeout=20)
    if r.status_code == 200:
        for coll in r.json():
            if coll.get("data", {}).get("name") == name:
                return coll.get("key")
    if dry_run:
        print(f"[DRY RUN] Would create collection: {name}"); return None
    payload = [{"name": name, "parentCollection": False}]
    r = safe_post(f"{API_BASE}/collections", headers=HEADERS_JSON, json=payload, timeout=20)
    if r.status_code in (200, 201):
        r2 = requests.get(f"{API_BASE}/collections", headers=HEADERS, params={"q": name, "limit": 100}, timeout=20)
        if r2.status_code == 200:
            for coll in r2.json():
                if coll.get("data", {}).get("name") == name:
                    return coll.get("key")
    print("⚠️ Failed to create/find collection:", name, r.status_code, getattr(r, "text", "")); return None

def ensure_item_in_collection(item_key: str, collection_key: str, dry_run: bool):
    r = requests.get(f"{API_BASE}/items/{item_key}", headers=HEADERS, timeout=20)
    if r.status_code != 200:
        print("⚠️ Could not fetch item to set collection:", item_key, r.status_code); return
    data = r.json().get("data", {})
    version = r.headers.get("Last-Modified-Version")
    cols = data.get("collections", []) or []
    if collection_key in cols: return
    cols.append(collection_key); data["collections"] = cols
    if dry_run:
        print(f"[DRY RUN] Would add item {item_key} to collection {collection_key}"); return
    headers = dict(HEADERS_JSON)
    if version: headers["If-Unmodified-Since-Version"] = version
    r2 = safe_put(f"{API_BASE}/items/{item_key}", headers=headers, json=data, timeout=20)
    if r2.status_code not in (200, 201, 204):
        print("⚠️ Failed to add item to collection:", r2.status_code, r2.text)

# ---------- Zotero: items (search + create with cache + normalization) ----------
def search_item_by_title(title: str, author: Optional[str], cache: dict) -> Optional[str]:
    norm_t = normalize_title(title)
    cached = cache.get("_items", {}).get(norm_t)
    if cached: return cached
    r = requests.get(f"{API_BASE}/items", headers=HEADERS,
                     params={"q": title, "qmode": "title", "itemType": "book", "limit": 25}, timeout=20)
    if r.status_code != 200: return None
    want_title = norm_t; want_author = normalize_author(author)
    for it in r.json():
        data = it.get("data", it); key = data.get("key")
        got_title = normalize_title(data.get("title", ""))
        if got_title != want_title: continue
        if want_author:
            creators = data.get("creators", [])
            names = " ".join([c.get("lastName","") for c in creators if c.get("creatorType")=="author"])
            if want_author not in normalize_author(names): continue
        cache["_items"][want_title] = key; return key
    return None

# ----- Creation helpers: robust key extraction & fallback matching -----
def _debug_created_response(r):
    try: body = r.text[:600]
    except Exception: body = "<no text>"
    print(f"[DEBUG] POST /items status={r.status_code} "
          f"headers={{Location:{r.headers.get('Location')}, LMV:{r.headers.get('Last-Modified-Version')}}} "
          f"body_snip={body!r}")

def extract_created_key(response):
    try: data = response.json()
    except Exception: data = None
    if isinstance(data, dict):
        succ = data.get("successful")
        if isinstance(succ, dict):
            for _, v in succ.items():
                if isinstance(v, dict) and v.get("key"): return v["key"]
                if isinstance(v, str) and v: return v
    if isinstance(data, list) and data:
        first = data[0]
        if isinstance(first, dict):
            if first.get("key"): return first["key"]
            d = first.get("data")
            if isinstance(d, dict) and d.get("key"): return d["key"]
    loc = response.headers.get("Location") or response.headers.get("location")
    if loc and "/items/" in loc: return loc.rsplit("/items/", 1)[-1].strip()
    return None

def fetch_recent_and_match(title, author, last_mod_version, cache):
    try: since = max(int(last_mod_version) - 3, 0)
    except Exception: since = None
    params = {"limit": 25, "sort": "dateModified", "direction": "desc"}
    if since is not None: params["since"] = since
    r = requests.get(f"{API_BASE}/items", headers=HEADERS, params=params, timeout=20)
    if r.status_code != 200: return None
    want_title = normalize_title(title); want_author = normalize_author(author)
    for it in r.json():
        data = it.get("data", it); key = data.get("key")
        got_title = normalize_title(data.get("title", ""))
        if got_title != want_title: continue
        if want_author:
            creators = data.get("creators", [])
            names = " ".join([c.get("lastName","") for c in creators if c.get("creatorType")=="author"])
            if want_author not in normalize_author(names): continue
        cache["_items"][want_title] = key; return key
    return None

def find_or_create_book_item(title: str, author: Optional[str], collection_key: Optional[str],
                             dry_run: bool, cache: dict) -> Optional[str]:
    key = search_item_by_title(title, author, cache)
    if key: return key
    if dry_run:
        print(f"[DRY RUN] Would create book: {title}")
        return None
    creators = [{"creatorType":"author","firstName":"", "lastName": author}] if author else []
    data = {"itemType": "book", "title": title, "creators": creators}
    if collection_key: data["collections"] = [collection_key]
    r = safe_post(f"{API_BASE}/items", headers=HEADERS_JSON, json=[data], timeout=30)
    _debug_created_response(r)
    if r.status_code not in (200, 201):
        print("Failed to create book:", r.status_code, getattr(r, "text", "")); return None
    created_key = extract_created_key(r)
    if created_key:
        cache["_items"][normalize_title(title)] = created_key; return created_key
    lmv = r.headers.get("Last-Modified-Version") or r.headers.get("Last-Modified")
    for attempt in range(3):
        time.sleep(1.0 + attempt)
        key = fetch_recent_and_match(title, author, lmv, cache)
        if key: return key
    print("Created book but couldn't retrieve key via response or recent-items match."); return None

def create_note_for_item(item_key: str, note_html: str, dry_run: bool):
    payload = [{"itemType": "note", "parentItem": item_key, "note": note_html}]
    if dry_run:
        print("[DRY RUN] Would add note:", note_html[:80].replace("\n"," ") + ("..." if len(note_html) > 80 else ""))
        return
    r = safe_post(f"{API_BASE}/items", headers=HEADERS_JSON, json=payload, timeout=20)
    if r.status_code not in (200, 201):
        print("Failed to add note:", r.status_code, r.text)

# ---------- Utilities ----------
def highlight_hash(h: dict) -> str:
    m = hashlib.md5()
    m.update((h.get("text","") + "|" + (h.get("note") or "") + "|" + (h.get("location") or "")).encode("utf-8"))
    return m.hexdigest()

def note_html_from_highlight(h: dict) -> str:
    html = f"<blockquote>{h['text']}</blockquote>"
    if h.get("note"): html += f"<p><em>Note: {h['note']}</em></p>"
    if h.get("location"): html += f"<p><small>Location: {h['location']}</small></p>"
    return html

# ---------- Main ----------
if __name__ == "__main__":
    print(">>> Config DRY_RUN:", DRY_RUN)
    if not DRY_RUN: confirm_live_or_abort()

    vault = Path(OBSIDIAN_VAULT_PATH)
    print("Vault path:", OBSIDIAN_VAULT_PATH)
    print("exists:", vault.exists(), "is_dir:", vault.is_dir())

    md_files = sorted(vault.rglob("*.md"))
    print(f"Found {len(md_files)} markdown files (recursive).")
    if not md_files:
        print("No markdown files found. Copy notes to On My iPad → Pyto → AmazonNotes."); sys.exit(0)

    collection_key = get_or_create_collection(COLLECTION_NAME, DRY_RUN)
    if DRY_RUN and collection_key is None:
        print(f"[DRY RUN] (Collection '{COLLECTION_NAME}' will be created or used)")

    sent_log = load_sent_log(OBSIDIAN_VAULT_PATH)
    done_set = set(sent_log.get("_done_titles", []))  # normalized titles

    processed = 0
    matched = 0

    for md in md_files:
        title, author, highlights = parse_kindle_md(md)
        if not title: continue
        if not title_matches_filters(title): continue
        matched += 1

        norm_t = normalize_title(title)

        # Resume: skip already completed titles (only when actually writing)
        if RESUME_ENABLED and not DRY_RUN and norm_t in done_set:
            print(f"\nSkipping (already completed in previous run): {title}")
            continue

        print(f"\nProcessing: {title} ({author}) — {len(highlights)} highlights")

        item_key = find_or_create_book_item(title, author, collection_key, DRY_RUN, sent_log)

        if item_key:
            if collection_key and not DRY_RUN:
                ensure_item_in_collection(item_key, collection_key, DRY_RUN)
        else:
            msg = "[DRY RUN] Skipping notes — no Zotero item (as expected)." if DRY_RUN else "Created book but no key — skipping notes."
            print(msg); continue

        # Per-title duplicate prevention
        sent_log.setdefault(title, [])
        already = set(sent_log[title])

        created_count = 0
        for h in highlights:
            h_id = highlight_hash(h)
            if h_id in already:
                print(f"  Skipping duplicate: {h['text'][:60]}..."); continue
            html = note_html_from_highlight(h)
            create_note_for_item(item_key, html, DRY_RUN)
            if not DRY_RUN:
                sent_log[title].append(h_id); created_count += 1

        if not DRY_RUN:
            print(f"  New notes created: {created_count}")
            # Mark this title as completed
            if norm_t not in done_set:
                sent_log["_done_titles"].append(norm_t); done_set.add(norm_t)
            save_sent_log(OBSIDIAN_VAULT_PATH, sent_log)

        processed += 1
        if BATCH_LIMIT and processed >= BATCH_LIMIT:
            print(f"\nReached BATCH_LIMIT={BATCH_LIMIT}. Stopping early."); break

    print(f"\nDone. Matched titles: {matched}, Processed: {processed}.")