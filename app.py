# app.py — v15 + retry-queue patch
import asyncio
import atexit
import builtins
import gc
import json
import logging
import os
import shutil
import signal
import subprocess
import time
import zipfile
from datetime import datetime, timezone, timedelta
from logging.handlers import RotatingFileHandler
from pathlib import Path

from telethon import TelegramClient, events
from telethon.sessions import StringSession
from telethon.tl.types import MessageMediaDocument, MessageMediaPhoto
from telethon.tl.functions.channels import JoinChannelRequest, GetParticipantRequest
from telethon.errors import (
    FloodWaitError, ChannelPrivateError, UsernameNotOccupiedError,
    ChatWriteForbiddenError, AuthKeyUnregisteredError, SessionRevokedError,
    AuthKeyDuplicatedError,
)

gc.set_threshold(20000, 25, 25)
_rp = builtins.print

API_ID         = int(os.environ["TG_API_ID"])
API_HASH       = os.environ["TG_API_HASH"]
STRING_SESSION = os.environ["TG_STRING_SESSION"]
HOME_DIR       = Path(os.environ.get("TG_HOME", str(Path.home() / "tg-pull")))

CONFIG_FILE = HOME_DIR / "config.json"
STATE_FILE  = HOME_DIR / "state/pull_state.json"
LOG_FILE    = HOME_DIR / "run.log"
DL_DIR      = HOME_DIR / "_downloads"
PID_FILE    = HOME_DIR / ".pid"
EXPORT_TMP  = Path("/tmp/tg-pull-export.zip")
for p in (STATE_FILE.parent, DL_DIR):
    p.mkdir(parents=True, exist_ok=True)

REPLY_PREFIX = "[tg-pull]"
WIZARD_TIMEOUT_SEC = 1800
STATE_VERSION = 2
MAX_RANGE = 1_000_000

_STATE_TRUNCATED = False

_log = logging.getLogger("tgpull")
_log.setLevel(logging.INFO)
if not _log.handlers:
    _h = RotatingFileHandler(str(LOG_FILE), maxBytes=5*1024*1024, backupCount=2, encoding="utf-8")
    _h.setFormatter(logging.Formatter("%(asctime)s %(message)s", datefmt="%Y-%m-%d %H:%M:%S"))
    _log.addHandler(_h)

def print(*a, **k):
    msg = " ".join(str(x) for x in a)
    try: _log.info(msg)
    except Exception: pass
    k.setdefault("flush", True)
    _rp(msg, **k)


def _pid_alive(pid):
    try: os.kill(pid, 0)
    except (OSError, ProcessLookupError): return False
    try:
        with open(f"/proc/{pid}/cmdline", "rb") as f:
            if "app.py" not in f.read().decode(errors="ignore"): return False
    except OSError: pass
    return True

def acquire_pid_lock():
    if PID_FILE.exists():
        try: old = int(PID_FILE.read_text().strip())
        except Exception: old = 0
        if old and old != os.getpid() and _pid_alive(old):
            print(f"[FATAL] another instance running (PID {old}) — exiting")
            raise SystemExit(1)
    PID_FILE.write_text(str(os.getpid()))

def release_pid_lock():
    try:
        if PID_FILE.exists():
            old = int(PID_FILE.read_text().strip())
            if old == os.getpid(): PID_FILE.unlink()
    except Exception: pass

atexit.register(release_pid_lock)

SHUTDOWN = False

def _sig(signum, _f):
    global SHUTDOWN
    SHUTDOWN = True
    print(f"[!] signal {signum}")
signal.signal(signal.SIGTERM, _sig)
signal.signal(signal.SIGINT,  _sig)

async def interruptible_sleep(secs):
    for _ in range(int(secs)):
        if SHUTDOWN: return
        await asyncio.sleep(1)


DEFAULT_CONFIG = {
    "configured": False,
    "source": "",
    "sources": [],
    "target": 0,
    "file_types": ["video"], "max_mb": 2000, "min_mb": 0,
    "silent_upload": True, "video_notes": False,
    "scan_log_every": 200, "state_save_every": 100,
    "disk_free_min_mb": 500, "keyword_blacklist": [], "paused": False,
    "throttle_enabled": True,
    "cpu_soft_pct": 50, "cpu_hard_pct": 80, "cpu_pause_pct": 90,
    "mem_soft_pct": 70, "mem_hard_pct": 85, "mem_pause_pct": 92,
    "check_every_n_msgs": 20,
    "soft_delay_sec": 3, "hard_delay_sec": 30, "pause_sleep_sec": 120,
    "chunk_kb_normal": 256, "chunk_kb_soft": 64, "chunk_kb_hard": 32,
    "schedule_enabled": False, "allowed_hours": [1,2,3,4,5,6],
    "tz_offset_hours": 5.5, "nice_level": 19, "ionice_idle": True,
    "scan_delay_sec": 0,
    "progress_notify_enabled": True,
    "progress_notify_every_videos": 10,
    "progress_notify_every_min": 30,
    "max_retries": 3,
    "dry_run": False,
}

_cfg_cache = {"data": None, "mtime": 0.0}

def load_config(force=False):
    global _cfg_cache
    if not CONFIG_FILE.exists():
        return _cfg_cache["data"] or dict(DEFAULT_CONFIG)
    try: m = CONFIG_FILE.stat().st_mtime
    except OSError: m = 0.0
    if not force and _cfg_cache["data"] is not None and m == _cfg_cache["mtime"]:
        return _cfg_cache["data"]
    try:
        cfg = json.loads(CONFIG_FILE.read_text())
        for k, v in DEFAULT_CONFIG.items(): cfg.setdefault(k, v)
        _cfg_cache["data"] = cfg; _cfg_cache["mtime"] = m
        return cfg
    except Exception as e:
        print(f"[!] config: {e}")
        return _cfg_cache["data"] or dict(DEFAULT_CONFIG)

def save_config(c):
    t = CONFIG_FILE.with_suffix(".tmp")
    t.write_text(json.dumps(c, indent=2))
    t.replace(CONFIG_FILE)
    _cfg_cache["data"] = None

def effective_sources(cfg):
    srcs = cfg.get("sources") or []
    if not srcs and cfg.get("source"):
        srcs = [cfg["source"]]
    return [s for s in srcs if s]


def _compact_ranges(ids):
    if not ids: return []
    s = sorted(set(ids))
    out, start, prev = [], s[0], s[0]
    for i in s[1:]:
        if i == prev + 1: prev = i
        else: out.append([start, prev]); start = prev = i
    out.append([start, prev])
    return out

def _expand_ranges(ranges):
    global _STATE_TRUNCATED
    out = set()
    for entry in (ranges or []):
        try: a, b = int(entry[0]), int(entry[1])
        except Exception: continue
        if b < a: continue
        if (b - a) > MAX_RANGE:
            print(f"[!] state range too large: [{a},{b}] — clamped")
            _STATE_TRUNCATED = True
            b = a + MAX_RANGE
        out.update(range(a, b + 1))
    return list(out)

def _state_blank():
    return {"v": STATE_VERSION, "sources": {}, "retry": [], "bw_dl": 0, "bw_ul": 0}

def _migrate_state(raw, cfg):
    if not isinstance(raw, dict): return _state_blank()
    if raw.get("v") == STATE_VERSION:
        out = _state_blank()
        for url, sd in raw.get("sources", {}).items():
            done = set(_expand_ranges(sd.get("done_ranges", [])))
            out["sources"][url] = {"last_id": sd.get("last_id", 0), "done": done}
        out["retry"] = raw.get("retry", [])
        out["bw_dl"] = raw.get("bw_dl", 0)
        out["bw_ul"] = raw.get("bw_ul", 0)
        return out
    src = cfg.get("source", "") or (effective_sources(cfg) or [""])[0]
    out = _state_blank()
    if src:
        ranges = raw.get("ranges")
        if ranges is None and raw.get("done"):
            ranges = _compact_ranges(raw["done"])
        out["sources"][src] = {"last_id": raw.get("last_id", 0), "done": set(_expand_ranges(ranges or []))}
    return out

def _serialize_state(s):
    out = {"v": STATE_VERSION, "sources": {}, "retry": s.get("retry", []),
           "bw_dl": s.get("bw_dl", 0), "bw_ul": s.get("bw_ul", 0)}
    for url, sd in s.get("sources", {}).items():
        out["sources"][url] = {
            "last_id": sd.get("last_id", 0),
            "done_ranges": _compact_ranges(list(sd.get("done", set()))),
        }
    return out

STATE = _state_blank()
_last_save_ts = 0.0

def load_state(cfg):
    global STATE
    if STATE_FILE.exists():
        try:
            raw = json.loads(STATE_FILE.read_text())
            STATE = _migrate_state(raw, cfg)
        except Exception as e:
            print(f"[!] state load: {e}"); STATE = _state_blank()
    else:
        STATE = _state_blank()
    return STATE

def save_state(force=False):
    global _last_save_ts
    now = time.time()
    if not force and now - _last_save_ts < 1.0: return
    try:
        payload = _serialize_state(STATE)
        t = STATE_FILE.with_suffix(".tmp")
        t.write_text(json.dumps(payload, separators=(",", ":")))
        t.replace(STATE_FILE)
        _last_save_ts = now
    except Exception as e:
        print(f"[!] save_state failed: {e}")


_cpu_count = os.cpu_count() or 1

def cpu_load_pct():
    try:
        with open("/proc/loadavg") as f:
            la = float(f.read().split()[0])
        return min(100.0, (la / _cpu_count) * 100.0)
    except Exception: return 0.0

def mem_used_pct():
    try:
        info = {}
        with open("/proc/meminfo") as f:
            for line in f:
                k, v = line.split(":", 1)
                info[k.strip()] = int(v.split()[0])
        total = info.get("MemTotal", 0)
        avail = info.get("MemAvailable", info.get("MemFree", 0))
        if total <= 0: return 0.0
        return ((total - avail) / total) * 100.0
    except Exception: return 0.0

def disk_free_mb():
    return shutil.disk_usage(str(DL_DIR)).free // (1024 * 1024)

def current_throttle_level(cfg):
    if not cfg.get("throttle_enabled", True): return "normal"
    cpu, mem = cpu_load_pct(), mem_used_pct()
    if cpu >= cfg["cpu_pause_pct"] or mem >= cfg["mem_pause_pct"]: return "pause"
    if cpu >= cfg["cpu_hard_pct"]  or mem >= cfg["mem_hard_pct"]:  return "hard"
    if cpu >= cfg["cpu_soft_pct"]  or mem >= cfg["mem_soft_pct"]:  return "soft"
    return "normal"

def chunk_size_for(cfg, level):
    return {"normal": cfg["chunk_kb_normal"], "soft": cfg["chunk_kb_soft"],
            "hard": cfg["chunk_kb_hard"],   "pause": cfg["chunk_kb_hard"]}.get(level, 64)

def delay_for(cfg, level):
    return {"normal": 0, "soft": cfg["soft_delay_sec"],
            "hard": cfg["hard_delay_sec"], "pause": cfg["pause_sleep_sec"]}.get(level, 0)

def in_schedule_window(cfg):
    if not cfg.get("schedule_enabled"): return True
    tz = timezone(timedelta(hours=cfg["tz_offset_hours"]))
    h = datetime.now(tz).hour
    hours = cfg.get("allowed_hours", [])
    return True if not hours else (h in hours)


class AdaptiveChunk:
    MIN_KB, MAX_KB = 32, 1024
    def __init__(self): self.cur = 64
    def get(self, level, cfg):
        if level == "soft": return cfg["chunk_kb_soft"]
        if level == "hard": return cfg["chunk_kb_hard"]
        return self.cur
    def report(self, nbytes, secs):
        kbps = (nbytes / 1024) / max(0.1, secs)
        if kbps > 500: self.cur = min(self.MAX_KB, self.cur * 2)
        elif kbps < 100: self.cur = max(self.MIN_KB, self.cur // 2)

_adaptive = AdaptiveChunk()


def _is_round(doc):
    for a in getattr(doc, "attributes", []):
        if type(a).__name__ == "VideoSize" and getattr(a, "is_round", False):
            return True
    return False

def classify(msg, cfg):
    if isinstance(msg.media, MessageMediaPhoto):
        return ("photo", "image/jpeg", getattr(msg.photo, "size", 0) or 0)
    if not isinstance(msg.media, MessageMediaDocument): return None
    doc = msg.media.document
    mime = getattr(doc, "mime_type", "") or ""
    size = doc.size or 0
    if mime.startswith("video/"):
        if not cfg["video_notes"] and _is_round(doc): return None
        return ("video", mime, size)
    if mime.startswith("image/"): return ("photo", mime, size)
    if mime.startswith("audio/"): return ("audio", mime, size)
    return ("document", mime, size)

def is_wanted(msg, cfg):
    c = classify(msg, cfg)
    if not c: return None
    ftype, mime, size = c
    if ftype not in cfg["file_types"]: return None
    mb = size // (1024 * 1024)
    if cfg["max_mb"] and mb > cfg["max_mb"]: return None
    if cfg["min_mb"] and mb < cfg["min_mb"]: return None
    bl = cfg["keyword_blacklist"]
    if bl:
        name = getattr(getattr(msg, "file", None), "name", "") or ""
        tl = ((msg.text or "") + " " + name).lower()
        for kw in bl:
            if kw.lower() in tl: return None
    return (ftype, mime, size)


STATS = {"scanned": 0, "found": 0, "moved": 0, "skipped": 0,
         "current_id": 0, "started": 0, "status": "idle",
         "last_error": "", "throttle": "normal"}

NOTIFY = {"last_videos": 0, "last_time": 0.0}


def _human_bytes(n):
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if n < 1024: return f"{n:.1f} {unit}"
        n /= 1024
    return f"{n:.1f} PB"

async def maybe_notify(cfg, force=False):
    if not cfg.get("progress_notify_enabled", True): return
    now = time.time()
    n_vid = cfg.get("progress_notify_every_videos", 10)
    n_min = cfg.get("progress_notify_every_min", 30)
    moved_delta = STATS["moved"] - NOTIFY["last_videos"]
    time_delta = now - NOTIFY["last_time"]
    if not force and moved_delta < n_vid and time_delta < n_min * 60: return
    try:
        await reply(
            f"progress\n"
            f"moved: {STATS['moved']}  |  found: {STATS['found']}  |  scanned: {STATS['scanned']}\n"
            f"dl: {_human_bytes(STATE.get('bw_dl', 0))}  |  ul: {_human_bytes(STATE.get('bw_ul', 0))}\n"
            f"disk_free: {disk_free_mb()} MB  |  throttle: {STATS['throttle']}"
        )
        NOTIFY["last_videos"] = STATS["moved"]
        NOTIFY["last_time"] = now
    except Exception as e:
        print(f"[!] notify: {e}")


WIZARD = {"active": False, "step": None, "data": {}, "last_activity": 0.0}

def wizard_touch():
    WIZARD["last_activity"] = time.time()

def wizard_prompt(step, cur_cfg):
    if step == "source":
        return ("1/7  මූලාශ්‍ර channel එක\n"
                "URL හෝ ID එවන්න.\n"
                "  • Public  :  https://t.me/channelname  හෝ  @channelname\n"
                "  • Private :  https://t.me/+InviteHash  හෝ  -100xxxxxxxxxx\n"
                "  • Multiple: comma එකෙන් වෙන් කරන්න (ch1,ch2)\n"
                "\nනවත්තන්න: /cancel    defaults: /defaults")
    if step == "target":
        return ("2/7  Target channel එක (upload වෙන තැන)\n"
                "Format: -100xxxxxxxxxx\n\n"
                "⚠️  ඔයාගේ account එක admin වෙන්න ඕන\n"
                "\nනවත්තන්න: /cancel")
    if step == "types":
        return ("3/7  File types — video,photo,audio,document\n\nSkip: /skip   Cancel: /cancel")
    if step == "maxmb":
        return ("4/7  උපරිම file size (MB). උදා:  2000\n\nSkip: /skip")
    if step == "minmb":
        return ("5/7  අවම file size (MB). උදා:  0\n\nSkip: /skip")
    if step == "silent":
        return ("6/7  Silent upload ද? Reply:  yes   හෝ   no")
    if step == "schedule":
        return ("7/7  Schedule — off / 1-6 / 22-23,0-5\n\n24/7:  off")
    return "done"

def _resume_scan():
    c = load_config(force=True); c["paused"] = False; save_config(c)

def wizard_handle(text, cur_cfg):
    wizard_touch()
    step = WIZARD["step"]
    low = text.strip().lower()
    t = text.strip()
    if low.startswith("/skip"): t = None

    if low.startswith("/cancel"):
        WIZARD.update({"active": False, "step": None, "data": {}})
        _resume_scan()
        return ("setup cancel — scan resume වුනා. /setup කරන්න පුළුවන්.", True)

    if low.startswith("/defaults"):
        c = dict(DEFAULT_CONFIG); c["paused"] = True
        save_config(c); _entity_cache.clear()
        WIZARD["active"] = True; WIZARD["step"] = "source"; WIZARD["data"] = {}
        wizard_touch()
        return ("bundled defaults reset — setup එක ආයෙත්\n\n" + wizard_prompt("source", c), False)

    if step == "source":
        if t is None or not t: return ("මූලාශ්‍ර channel හිස් වෙන්න බෑ.", False)
        srcs = [x.strip() for x in t.split(",") if x.strip()]
        if not srcs: return ("මූලාශ්‍ර channel හිස් වෙන්න බෑ.", False)
        WIZARD["data"]["sources"] = srcs; WIZARD["step"] = "target"
        return (wizard_prompt("target", cur_cfg), False)
    if step == "target":
        if t is None: return ("Target හිස් වෙන්න බෑ.", False)
        try: tid = int(t)
        except ValueError: return ("Number එකක් එවන්න.", False)
        if tid == 0: return ("Target 0 වෙන්න බෑ — numeric channel id එකක් එවන්න.", False)
        WIZARD["data"]["target"] = tid; WIZARD["step"] = "types"
        return (wizard_prompt("types", cur_cfg), False)
    if step == "types":
        if t is None: WIZARD["data"]["file_types"] = cur_cfg["file_types"]
        else:
            types = [x.strip().lower() for x in t.split(",") if x.strip()]
            valid = {"video", "photo", "audio", "document"}
            if not types or not all(x in valid for x in types):
                return ("Invalid — video,photo,audio,document.", False)
            WIZARD["data"]["file_types"] = types
        WIZARD["step"] = "maxmb"; return (wizard_prompt("maxmb", cur_cfg), False)
    if step == "maxmb":
        if t is None: WIZARD["data"]["max_mb"] = cur_cfg["max_mb"]
        else:
            try: WIZARD["data"]["max_mb"] = int(t)
            except ValueError: return ("Number එකක්.", False)
        WIZARD["step"] = "minmb"; return (wizard_prompt("minmb", cur_cfg), False)
    if step == "minmb":
        if t is None: WIZARD["data"]["min_mb"] = cur_cfg["min_mb"]
        else:
            try: WIZARD["data"]["min_mb"] = int(t)
            except ValueError: return ("Number එකක්.", False)
        WIZARD["step"] = "silent"; return (wizard_prompt("silent", cur_cfg), False)
    if step == "silent":
        if t is None: WIZARD["data"]["silent_upload"] = cur_cfg["silent_upload"]
        else:
            v = t.lower()
            if v in ("yes","y","1","true","ඔව්"): WIZARD["data"]["silent_upload"] = True
            elif v in ("no","n","0","false","නෑ"): WIZARD["data"]["silent_upload"] = False
            else: return ("'yes' හෝ 'no'.", False)
        WIZARD["step"] = "schedule"; return (wizard_prompt("schedule", cur_cfg), False)
    if step == "schedule":
        if t is None or t.lower() == "off":
            WIZARD["data"]["schedule_enabled"] = False
            WIZARD["data"]["allowed_hours"] = cur_cfg["allowed_hours"]
        else:
            try:
                hours = []
                for part in t.split(","):
                    if "-" in part:
                        a, b = part.split("-"); hours.extend(range(int(a), int(b) + 1))
                    else: hours.append(int(part))
                WIZARD["data"]["allowed_hours"] = sorted(set(h % 24 for h in hours))
                WIZARD["data"]["schedule_enabled"] = True
            except Exception: return ("Invalid format.", False)
        cfg = load_config(force=True)
        cfg.update(WIZARD["data"]); cfg["configured"] = True; cfg["paused"] = False
        save_config(cfg)
        _entity_cache.clear()
        d = WIZARD["data"]
        sched_txt = f"{d.get('allowed_hours')}" if d.get("schedule_enabled") else "off (24/7)"
        summary = ("✅  Setup සම්පූර්ණයි\n\n"
                   f"Sources        : {d.get('sources')}\n"
                   f"Target         : {d.get('target')}\n"
                   f"File types     : {d.get('file_types')}\n"
                   f"Max / Min size : {d.get('max_mb')} / {d.get('min_mb')} MB\n"
                   f"Silent upload  : {d.get('silent_upload')}\n"
                   f"Schedule       : {sched_txt}\n\n"
                   "Scan පටන් ගන්නවා...  /help")
        WIZARD.update({"active": False, "step": None, "data": {}})
        return (summary, True)
    return ("Wizard error — /cancel.", True)


client = TelegramClient(
    StringSession(STRING_SESSION), API_ID, API_HASH,
    connection_retries=5, retry_delay=3,
    auto_reconnect=True, request_retries=5,
    flood_sleep_threshold=60,
    sequential_updates=True,
    use_ipv6=False, timeout=30,
)

_entity_cache = {}
async def get_cached_entity(ref):
    key = str(ref)
    if key in _entity_cache: return _entity_cache[key]
    e = await client.get_entity(ref)
    _entity_cache[key] = e
    return e

async def reply(text):
    try: await client.send_message("me", f"{REPLY_PREFIX}\n{text}")
    except Exception as e: print(f"[!] reply: {e}")


async def stream_download(msg, path, chunk_kb):
    total = 0
    with open(path, "wb") as f:
        async for chunk in client.iter_download(msg.media, request_size=chunk_kb * 1024):
            f.write(chunk); total += len(chunk)
    return total


class _ScanAbort(Exception):
    pass


async def process_one_video(cfg, msg, target, source_key, wanted=None, count_found=True):
    if wanted is None:
        wanted = is_wanted(msg, cfg)
    if not wanted: return True
    if count_found:
        STATS["found"] += 1
    ftype, mime, size = wanted
    mb = size // (1024 * 1024)
    sd = STATE["sources"].setdefault(source_key, {"last_id": 0, "done": set()})

    if cfg.get("dry_run"):
        print(f"[dry] {msg.id} ({mb} MB, {ftype})")
        return True

    if disk_free_mb() < mb + cfg["disk_free_min_mb"]:
        for f in DL_DIR.glob("*"):
            try: f.unlink()
            except OSError: pass
        gc.collect()
        if disk_free_mb() < mb + cfg["disk_free_min_mb"]:
            print(f"[!] disk tight — skip {msg.id}"); return False

    ext = {"video":"mp4","photo":"jpg","audio":"mp3","document":"bin"}.get(ftype, "bin")
    path = DL_DIR / f"m_{msg.id}.{ext}"
    chunk = _adaptive.get(STATS["throttle"], cfg)
    print(f"[+] {ftype} {msg.id} ({mb} MB, #{STATS['found']}) chunk:{chunk}KB")
    try:
        t0 = time.time()
        n = await stream_download(msg, path, chunk)
        _adaptive.report(n, time.time() - t0)
        STATS["bw_dl"] = STATE["bw_dl"] = STATE.get("bw_dl", 0) + n
        print(f"[↓] {msg.id}")

        await client.send_file(target, str(path), caption=None,
                               supports_streaming=(ftype == "video"),
                               silent=cfg["silent_upload"],
                               force_document=(ftype == "document"))
        STATS["bw_ul"] = STATE["bw_ul"] = STATE.get("bw_ul", 0) + size
        print(f"[↑] {msg.id}")

        sd["done"].add(msg.id); STATS["moved"] += 1
        return True
    except FloodWaitError as e:
        print(f"[!] flood {e.seconds}s"); await interruptible_sleep(e.seconds + 5); return False
    except ChatWriteForbiddenError:
        STATS["last_error"] = "no write permission"
        try: await reply("target channel එකට write permission නෑ — scan එක නැවතුනා.\n"
                         "admin fix කරලා /resume කරන්න")
        except Exception: pass
        try:
            c = load_config(force=True); c["paused"] = True; save_config(c)
        except Exception: pass
        raise _ScanAbort("ChatWriteForbidden")
    except (AuthKeyUnregisteredError, SessionRevokedError, AuthKeyDuplicatedError):
        raise
    except Exception as e:
        print(f"[!] {msg.id}: {e}"); STATS["last_error"] = str(e); return False
    finally:
        try: path.unlink()
        except OSError: pass
        gc.collect()


def _add_to_retry(source_key, msg_id):
    retry = STATE.setdefault("retry", [])
    for entry in retry:
        if entry[0] == msg_id and entry[1] == source_key: return
    retry.append([msg_id, source_key, 1]); save_state()


async def process_retry_queue(cfg, target):
    cfg = load_config(force=True)
    retry = STATE.get("retry", [])
    if not retry: return
    print(f"[~] retry queue: {len(retry)}")
    try: await reply(f"retrying {len(retry)} failed videos...")
    except Exception: pass
    max_r = cfg.get("max_retries", 3)
    remaining = []

    for entry in retry:
        if SHUTDOWN: remaining.append(entry); continue
        while load_config().get("paused") and not SHUTDOWN:
            await interruptible_sleep(15)
        if SHUTDOWN: remaining.append(entry); continue

        try: msg_id, src_key, attempts = entry[0], entry[1], entry[2]
        except Exception: continue
        try:
            source = await get_cached_entity(src_key)
            msg = await client.get_messages(source, ids=msg_id)
            if msg is None: continue
            ok = await process_one_video(cfg, msg, target, src_key, count_found=False)
            if ok:
                save_state()
            else:
                attempts += 1
                if attempts >= max_r:
                    sd = STATE["sources"].setdefault(src_key, {"last_id": 0, "done": set()})
                    sd["done"].add(msg_id); save_state()
                else:
                    remaining.append([msg_id, src_key, attempts])
        except _ScanAbort:
            remaining.append(entry)
            # preserve unprocessed entries
            try:
                idx = retry.index(entry)
                remaining.extend(retry[idx + 1:])
            except ValueError:
                pass
            STATE["retry"] = remaining; save_state(force=True)
            raise
        except (AuthKeyUnregisteredError, SessionRevokedError, AuthKeyDuplicatedError):
            raise
        except Exception as e:
            print(f"[!] retry {entry}: {e}")
            remaining.append([entry[0], entry[1], entry[2] + 1])
        await interruptible_sleep(3)
    STATE["retry"] = remaining; save_state(force=True)
    print(f"[~] retry done — {len(remaining)} remaining")


async def scan_source(cfg, source_url, target):
    try:
        source = await get_cached_entity(source_url)
    except Exception as e:
        print(f"[!] resolve {source_url}: {e}")
        try: await reply(f"cannot resolve source:\n{source_url}\n\n{e}")
        except Exception: pass
        return

    print(f"[+] source: {getattr(source, 'title', source.id)}")
    try:
        await client(JoinChannelRequest(source)); print("[+] joined")
    except Exception as e: print(f"[i] join: {e}")

    sd = STATE["sources"].setdefault(source_url, {"last_id": 0, "done": set()})
    done = sd["done"]; last_id = sd["last_id"]
    print(f"[~] resume: > {last_id}  |  done: {len(done)}")

    try: scan_mtime = CONFIG_FILE.stat().st_mtime
    except OSError: scan_mtime = 0.0

    iterator = client.iter_messages(source, min_id=last_id, reverse=True)

    while not SHUTDOWN:
        try: cur_mtime = CONFIG_FILE.stat().st_mtime
        except OSError: cur_mtime = scan_mtime
        if cur_mtime != scan_mtime:
            print("[~] config changed mid-scan — aborting source scan")
            save_state(force=True)
            return

        while not SHUTDOWN:
            cfg = load_config()
            if not cfg.get("paused"): break
            STATS["status"] = "paused"; STATS["throttle"] = "paused"
            if WIZARD["active"] and time.time() - WIZARD.get("last_activity", 0) > WIZARD_TIMEOUT_SEC:
                WIZARD.update({"active": False, "step": None, "data": {}})
                c = load_config(force=True); c["paused"] = False; save_config(c)
                try: await reply("wizard timeout — scan resume")
                except Exception: pass
                break
            await interruptible_sleep(15)
        if SHUTDOWN: save_state(force=True); return

        if STATS["scanned"] % cfg["check_every_n_msgs"] == 0:
            cfg = load_config()
            if not in_schedule_window(cfg):
                STATS["status"] = "outside-window"; STATS["throttle"] = "schedule"
                await interruptible_sleep(300)
                continue
            lvl = current_throttle_level(cfg); STATS["throttle"] = lvl
            if lvl == "pause":
                await interruptible_sleep(cfg["pause_sleep_sec"])
                continue
            elif lvl in ("soft", "hard"):
                await interruptible_sleep(delay_for(cfg, lvl))

        try:
            msg = await iterator.__anext__()
        except StopAsyncIteration:
            break

        STATS["scanned"] += 1
        STATS["current_id"] = msg.id
        if not cfg.get("dry_run"):
            sd["last_id"] = msg.id

        if STATS["scanned"] % cfg["scan_log_every"] == 0:
            print(f"[scan] {STATS['scanned']} | found:{STATS['found']} | moved:{STATS['moved']} | id:{msg.id}")
        if STATS["scanned"] % cfg["state_save_every"] == 0:
            save_state()

        wanted = is_wanted(msg, cfg)
        if not wanted: continue
        if msg.id in done: continue
        if getattr(msg.media, "ttl_seconds", None):
            if not cfg.get("dry_run"):
                done.add(msg.id); save_state()
            continue

        try:
            ok = await process_one_video(cfg, msg, target, source_url, wanted=wanted)
            if ok:
                if not cfg.get("dry_run"):
                    save_state()
                    await maybe_notify(cfg)
            else:
                if not cfg.get("dry_run"):
                    _add_to_retry(source_url, msg.id)
        except _ScanAbort:
            print("[!] scan abort signal received")
            save_state(force=True)
            raise

    save_state(force=True)


async def run_session():
    cfg = load_config()
    if not cfg.get("target"):
        try: await reply("target channel set කරන්න — /target <id>  හෝ /setup")
        except Exception: pass
        return

    sources = effective_sources(cfg)
    original_sources = list(sources)
    if not sources:
        try: await reply("no source configured — /setup කරන්න")
        except Exception: pass
        return

    try:
        target = await get_cached_entity(cfg["target"])
    except Exception as e:
        print(f"[!] target: {e}")
        try: await reply(f"cannot resolve target: {cfg['target']}\n{e}\n\n/target <id>")
        except Exception: pass
        return

    print(f"[+] target: {getattr(target, 'title', target.id)}")
    try:
        p = await client(GetParticipantRequest(target, "me"))
        print(f"[+] target access: {type(p.participant).__name__}")
    except Exception as e: print(f"[!] target access: {e}")

    if cfg.get("dry_run"):
        try: await reply("DRY-RUN mode — download/upload නෑ")
        except Exception: pass

    STATS["started"] = time.time(); STATS["status"] = "scanning"

    for src_url in sources:
        if SHUTDOWN: break
        cur_cfg = load_config()
        if cur_cfg.get("paused") or effective_sources(cur_cfg) != original_sources:
            print("[~] config changed between sources — aborting run_session")
            save_state(force=True)
            return
        print(f"[>>] source: {src_url}")
        try:
            await scan_source(cfg, src_url, target)
        except _ScanAbort:
            print("[!] scan aborted (write permission) — exiting run_session")
            return
        except (AuthKeyUnregisteredError, SessionRevokedError, AuthKeyDuplicatedError):
            raise
        except Exception as e:
            print(f"[!] source {src_url}: {e}")

    if not cfg.get("dry_run"):
        cur_cfg = load_config()
        if cur_cfg.get("paused") or effective_sources(cur_cfg) != original_sources:
            print("[~] config changed before retry — skipping retry queue")
            save_state(force=True)
            return
        try:
            await process_retry_queue(cfg, target)
        except _ScanAbort:
            print("[!] retry aborted (write permission)")
            return
        except (AuthKeyUnregisteredError, SessionRevokedError, AuthKeyDuplicatedError):
            raise
        except Exception as e:
            print(f"[!] retry queue: {e}")

    await maybe_notify(cfg, force=True)
    STATS["status"] = "done"
    print(f"[✓] done — moved={STATS['moved']}")


def set_low_priority(cfg):
    if cfg.get("ionice_idle", True):
        try:
            r = subprocess.run(["which", "ionice"], capture_output=True)
            if r.returncode == 0:
                subprocess.run(["ionice", "-c", "3", "-p", str(os.getpid())], check=False)
                print("[+] ionice idle")
        except Exception as e: print(f"[!] ionice: {e}")


async def do_export():
    try:
        if EXPORT_TMP.exists(): EXPORT_TMP.unlink()
        with zipfile.ZipFile(str(EXPORT_TMP), "w", zipfile.ZIP_DEFLATED) as z:
            if CONFIG_FILE.exists(): z.write(str(CONFIG_FILE), "config.json")
            if STATE_FILE.exists(): z.write(str(STATE_FILE), "pull_state.json")
        await client.send_file("me", str(EXPORT_TMP),
                               caption=f"{REPLY_PREFIX}\nexport (config + state)")
        return True
    except Exception as e:
        print(f"[!] export: {e}"); return False
    finally:
        try:
            if EXPORT_TMP.exists(): EXPORT_TMP.unlink()
        except Exception: pass


@client.on(events.NewMessage(from_users="me", chats="me", pattern=r"^/"))
async def on_command(event):
    try:
        txt = (event.raw_text or "").strip()
        if REPLY_PREFIX in txt[:20]: return
        parts = txt.split()
        cmd = parts[0][1:].lower() if parts else ""
        args = parts[1:]

        if WIZARD["active"]:
            if cmd in ("skip", "cancel", "defaults"): return
            if cmd not in ("setup", "help", "stop", "config", "export"):
                await reply(f"wizard active — '/{cmd}' වැඩ කරන්නේ නෑ.\nwizard complete හෝ /cancel.")
                return

        cfg = load_config()

        if cmd == "help":
            await reply("SETUP\n/setup  /defaults  /config  /cancel\n"
                        "INFO\n/status  /load  /stats\n"
                        "CONFIG\n/source <url>  /sources add|rm|list|clear <url>\n"
                        "/target <id>\n/types video,photo,audio,document\n"
                        "/maxmb <n>  /minmb <n>\n"
                        "/blacklist <word>  /unblacklist <word>\n/quiet\n"
                        "RESOURCE\n/throttle on|off  /cap cpu|mem <pct>\n"
                        "/schedule <1-6,22-23> | off\n"
                        "CONTROL\n/pause  /resume  /jump <id>  /rescan  /stop\n"
                        "TOOLS\n/dry-run on|off  /retry  /export")

        elif cmd == "setup":
            cfg["paused"] = True; save_config(cfg)
            WIZARD.update({"active": True, "step": "source", "data": {}})
            wizard_touch()
            await reply("scan paused — setup\n\n" + wizard_prompt("source", cfg))

        elif cmd == "defaults":
            c = dict(DEFAULT_CONFIG); c["paused"] = True
            save_config(c); _entity_cache.clear()
            WIZARD.update({"active": True, "step": "source", "data": {}})
            wizard_touch()
            await reply("defaults reset — setup ආයෙත්\n\n" + wizard_prompt("source", c))

        elif cmd == "cancel":
            await reply("cancel කරන්න දෙයක් නෑ.")

        elif cmd == "status":
            el = int(time.time() - STATS["started"]) if STATS["started"] else 0
            srcs = effective_sources(cfg)
            per_src = [f"  {s}: last_id={STATE['sources'].get(s,{}).get('last_id',0)} "
                       f"done={len(STATE['sources'].get(s,{}).get('done',set()))}" for s in srcs]
            await reply(
                f"status: {STATS['status']}\nthrottle: {STATS['throttle']}\n"
                f"cpu: {cpu_load_pct():.1f}%  mem: {mem_used_pct():.1f}%\n"
                f"scanned:{STATS['scanned']}  found:{STATS['found']}  moved:{STATS['moved']}\n"
                f"disk_free:{disk_free_mb()} MB\n"
                f"retry_queue:{len(STATE.get('retry', []))}\n"
                f"dry_run:{cfg.get('dry_run')}\n"
                f"elapsed:{el}s  last_error:{STATS['last_error'] or '-'}\n"
                f"sources:\n" + "\n".join(per_src))

        elif cmd == "load":
            lvl = current_throttle_level(cfg)
            await reply(f"cpu:{cpu_load_pct():.1f}%  mem:{mem_used_pct():.1f}%\n"
                        f"throttle:{lvl}\nwindow:{in_schedule_window(cfg)}\n"
                        f"disk_free:{disk_free_mb()}MB\n"
                        f"chunk_kb:{chunk_size_for(cfg, lvl)}  delay:{delay_for(cfg, lvl)}")

        elif cmd == "stats":
            await reply(
                f"bandwidth\n"
                f"downloaded: {_human_bytes(STATE.get('bw_dl', 0))}\n"
                f"uploaded  : {_human_bytes(STATE.get('bw_ul', 0))}\n"
                f"moved: {STATS['moved']}  |  found: {STATS['found']}  |  scanned: {STATS['scanned']}\n"
                f"retry queue: {len(STATE.get('retry', []))}")

        elif cmd == "config":
            await reply("config:\n" + json.dumps(cfg, indent=2))

        elif cmd == "source":
            if not args: await reply("usage: /source <url>"); return
            cfg["source"] = args[0]; cfg["sources"] = [args[0]]; cfg["paused"] = True
            save_config(cfg); _entity_cache.clear()
            STATE["sources"] = {}; save_state(force=True)
            await reply(f"sources = [{args[0]}]\nstate reset — /resume")

        elif cmd == "sources":
            if not args or args[0] == "list":
                srcs = effective_sources(cfg)
                await reply("sources:\n" + "\n".join(f"  {i+1}. {s}" for i, s in enumerate(srcs)) if srcs else "sources: (empty)")
                return
            action = args[0].lower()
            if action == "add" and len(args) >= 2:
                url = args[1]
                srcs = list(cfg.get("sources") or [])
                if not srcs and cfg.get("source"): srcs = [cfg["source"]]
                if url in srcs: await reply(f"already: {url}"); return
                srcs.append(url); cfg["sources"] = srcs; cfg["source"] = ""
                save_config(cfg); _entity_cache.clear()
                await reply(f"added: {url}\n{srcs}")
            elif action in ("rm","remove") and len(args) >= 2:
                url = args[1]
                srcs = list(cfg.get("sources") or [])
                if not srcs and cfg.get("source"): srcs = [cfg["source"]]
                if url not in srcs: await reply(f"not found: {url}"); return
                srcs.remove(url); cfg["sources"] = srcs
                save_config(cfg); _entity_cache.clear()
                await reply(f"removed: {url}\n{srcs}")
            elif action == "clear":
                cfg["sources"] = []; cfg["source"] = ""; save_config(cfg)
                await reply("sources cleared")
            else:
                await reply("usage: /sources add|rm|list|clear [url]")

        elif cmd == "target":
            if not args: await reply("usage: /target <id>"); return
            try:
                tid = int(args[0])
                if tid == 0: raise ValueError()
                cfg["target"] = tid
            except ValueError: await reply("valid numeric id"); return
            save_config(cfg); _entity_cache.clear(); await reply(f"target = {cfg['target']}")

        elif cmd == "types":
            if not args: await reply("usage: /types video,photo,audio,document"); return
            cfg["file_types"] = [t.strip().lower() for t in args[0].split(",") if t.strip()]
            save_config(cfg); await reply(f"file_types = {cfg['file_types']}")

        elif cmd == "maxmb":
            if not args: return
            try: cfg["max_mb"] = int(args[0])
            except ValueError: await reply("number"); return
            save_config(cfg); await reply(f"max_mb = {cfg['max_mb']}")

        elif cmd == "minmb":
            if not args: return
            try: cfg["min_mb"] = int(args[0])
            except ValueError: await reply("number"); return
            save_config(cfg); await reply(f"min_mb = {cfg['min_mb']}")

        elif cmd == "blacklist":
            if not args: return
            w = " ".join(args)
            if w not in cfg["keyword_blacklist"]:
                cfg["keyword_blacklist"].append(w); save_config(cfg)
            await reply(f"blacklist: {cfg['keyword_blacklist']}")

        elif cmd == "unblacklist":
            if not args: return
            w = " ".join(args)
            if w in cfg["keyword_blacklist"]:
                cfg["keyword_blacklist"].remove(w); save_config(cfg)
            await reply(f"blacklist: {cfg['keyword_blacklist']}")

        elif cmd == "quiet":
            cfg["silent_upload"] = not cfg["silent_upload"]; save_config(cfg)
            await reply(f"silent_upload = {cfg['silent_upload']}")

        elif cmd == "throttle":
            if not args: cfg["throttle_enabled"] = not cfg["throttle_enabled"]
            else: cfg["throttle_enabled"] = args[0].lower() in ("on","1","true","yes")
            save_config(cfg); await reply(f"throttle_enabled = {cfg['throttle_enabled']}")

        elif cmd == "cap":
            if len(args) < 2: await reply("usage: /cap cpu|mem <pct>"); return
            kind = args[0].lower()
            try: val = int(args[1])
            except ValueError: await reply("number"); return
            if kind == "cpu":
                cfg["cpu_hard_pct"] = val
                cfg["cpu_soft_pct"] = max(10, val - 20)
                cfg["cpu_pause_pct"] = min(99, val + 10)
            elif kind == "mem":
                cfg["mem_hard_pct"] = val
                cfg["mem_soft_pct"] = max(10, val - 15)
                cfg["mem_pause_pct"] = min(99, val + 7)
            else: await reply("kind: cpu හෝ mem"); return
            save_config(cfg)
            await reply(f"{kind} soft:{cfg[f'{kind}_soft_pct']} hard:{cfg[f'{kind}_hard_pct']} pause:{cfg[f'{kind}_pause_pct']}")

        elif cmd == "schedule":
            if not args or args[0].lower() == "off":
                cfg["schedule_enabled"] = False; save_config(cfg)
                await reply("schedule: off (24/7)"); return
            hours = []
            try:
                for part in args[0].split(","):
                    if "-" in part:
                        a, b = part.split("-"); hours.extend(range(int(a), int(b) + 1))
                    else: hours.append(int(part))
            except ValueError:
                await reply("invalid — off, 1-6, 22-23,0-5"); return
            cfg["allowed_hours"] = sorted(set(h % 24 for h in hours))
            cfg["schedule_enabled"] = True; save_config(cfg)
            await reply(f"schedule: {cfg['allowed_hours']}")

        elif cmd == "pause":
            cfg["paused"] = True; save_config(cfg); await reply("paused")

        elif cmd == "resume":
            cfg["paused"] = False; save_config(cfg); await reply("resumed")

        elif cmd == "jump":
            if len(args) < 1: await reply("usage: /jump <msg_id> [source_url]"); return
            try: mid = int(args[0])
            except ValueError: await reply("numeric id"); return
            src = args[1] if len(args) > 1 else (effective_sources(cfg) or [""])[0]
            if not src: await reply("no source"); return
            sd = STATE["sources"].setdefault(src, {"last_id": 0, "done": set()})
            sd["last_id"] = mid; save_state(force=True)
            await reply(f"{src} last_id = {mid}")

        elif cmd == "rescan":
            srcs = effective_sources(cfg)
            for s in srcs:
                STATE["sources"][s] = {"last_id": 0, "done": set()}
            STATE["retry"] = []; save_state(force=True)
            await reply("state reset")

        elif cmd == "dry-run":
            if not args: cfg["dry_run"] = not cfg["dry_run"]
            else: cfg["dry_run"] = args[0].lower() in ("on","1","true","yes")
            save_config(cfg)
            await reply(f"dry_run = {cfg['dry_run']}" + ("\n(download/upload skip — state uncommitted)" if cfg['dry_run'] else ""))

        elif cmd == "retry":
            n = len(STATE.get("retry", []))
            await reply(f"retry queue: {n} entries" if n else "retry queue: empty")

        elif cmd == "export":
            ok = await do_export()
            await reply("export sent ✓" if ok else "export failed — log check")

        elif cmd == "stop":
            SHUTDOWN = True; await reply("stopping")

        else:
            await reply(f"unknown: /{cmd} — /help")
    except Exception as e:
        print(f"[!] cmd: {e}")
        try: await reply(f"error: {e}")
        except Exception: pass


@client.on(events.NewMessage(from_users="me", chats="me"))
async def on_plain_message(event):
    try:
        txt = (event.raw_text or "").strip()
        if REPLY_PREFIX in txt[:20]: return
        if not WIZARD["active"]: return
        low = txt.lower()
        if txt.startswith("/") and not any(low.startswith(x)
                for x in ("/skip", "/cancel", "/defaults")): return
        cfg = load_config()
        reply_text, _ = wizard_handle(txt, cfg)
        await reply(reply_text)
    except Exception as e: print(f"[!] wizard: {e}")


def _wizard_timeout_check():
    if WIZARD["active"] and time.time() - WIZARD.get("last_activity", 0) > WIZARD_TIMEOUT_SEC:
        WIZARD.update({"active": False, "step": None, "data": {}})
        return True
    return False


async def wait_until_configured():
    last_wizard_start_seen = 0.0
    timeout_fired_for_start = None
    while not SHUTDOWN:
        if load_config().get("configured"): return
        cur_start = WIZARD.get("last_activity", 0.0)
        if WIZARD["active"] and cur_start != last_wizard_start_seen:
            last_wizard_start_seen = cur_start
            timeout_fired_for_start = None
        if WIZARD["active"] and timeout_fired_for_start != last_wizard_start_seen:
            if _wizard_timeout_check():
                print("[!] wizard timeout (startup)")
                timeout_fired_for_start = last_wizard_start_seen
                c = load_config(force=True); c["paused"] = False; save_config(c)
                try: await reply("wizard timeout — /setup කරන්න")
                except Exception: pass
        await interruptible_sleep(10)

IDLE_BACKOFFS = [60, 300, 900, 1800, 3600]


async def idle_backoff(secs, start_mtime):
    for _ in range(int(secs)):
        if SHUTDOWN: return False
        if WIZARD["active"]:
            print("[~] wizard active — abort idle")
            return True
        try: cur = CONFIG_FILE.stat().st_mtime
        except OSError: cur = start_mtime
        if cur != start_mtime:
            print("[~] config changed — abort idle")
            return True
        if _wizard_timeout_check():
            print("[!] wizard timeout (idle)")
            c = load_config(force=True); c["paused"] = False; save_config(c)
            try: await reply("wizard timeout — scan resume")
            except Exception: pass
            return True
        await asyncio.sleep(1)
    return False


async def main():
    acquire_pid_lock()
    try:
        await client.start()
    except (AuthKeyUnregisteredError, SessionRevokedError, AuthKeyDuplicatedError) as e:
        print(f"[FATAL] session invalid: {e}"); return
    except Exception as e:
        print(f"[FATAL] connect failed: {e}"); return

    cfg = load_config(force=True)
    set_low_priority(cfg)
    load_state(cfg)
    STATS["bw_dl"] = STATE.get("bw_dl", 0)
    STATS["bw_ul"] = STATE.get("bw_ul", 0)
    NOTIFY["last_time"] = time.time()

    if _STATE_TRUNCATED:
        try: await reply("⚠️  state file range was too large and got truncated")
        except Exception: pass

    if not cfg.get("configured"):
        WIZARD.update({"active": True, "step": "source", "data": {}})
        wizard_touch()
        try:
            await client.send_message("me",
                f"{REPLY_PREFIX}\nbot එක start වුනා\n"
                f"පළවෙනි run — setup අවශ්‍යයි\n\n"
                + wizard_prompt("source", cfg))
        except Exception as e: print(f"[!] startup: {e}")
    else:
        srcs = effective_sources(cfg)
        try:
            await client.send_message("me",
                f"{REPLY_PREFIX}\nbot start\n"
                f"sources: {srcs}\n"
                f"target: {cfg.get('target')}\n\nCommands: /help")
        except Exception as e: print(f"[!] startup: {e}")

    errors = 0; idle_idx = 0

    while not SHUTDOWN:
        try:
            await wait_until_configured()
            if SHUTDOWN: break

            if load_config().get("paused"):
                print("[~] paused — staying idle")
                STATS["status"] = "paused"
                try: idle_mtime = CONFIG_FILE.stat().st_mtime
                except OSError: idle_mtime = 0.0
                interrupted = await idle_backoff(30, idle_mtime)
                if interrupted: idle_idx = 0
                continue

            try: idle_mtime = CONFIG_FILE.stat().st_mtime
            except OSError: idle_mtime = 0.0

            await run_session()
            if SHUTDOWN: break
            STATS["status"] = "idle"

            wait = IDLE_BACKOFFS[min(idle_idx, len(IDLE_BACKOFFS)-1)]
            idle_idx += 1
            print(f"[~] idle {wait}s (backoff #{idle_idx})")
            interrupted = await idle_backoff(wait, idle_mtime)
            if interrupted:
                idle_idx = 0
            errors = 0
        except (AuthKeyUnregisteredError, SessionRevokedError, AuthKeyDuplicatedError):
            print("[FATAL] session invalid")
            try: await reply("FATAL: session invalid — regenerate StringSession")
            except Exception: pass
            break
        except KeyboardInterrupt: break
        except Exception as e:
            errors += 1
            print(f"[!] err#{errors}: {type(e).__name__}: {e}")
            STATS["last_error"] = str(e)
            try: await reply(f"error#{errors}: {e}")
            except Exception: pass
            await interruptible_sleep(min(30 * errors, 300))
            try: await client.connect()
            except Exception: pass

    save_state(force=True)
    try: await reply("stopped")
    except Exception: pass
    print("[.] exit")


with client:
    client.loop.run_until_complete(main())
