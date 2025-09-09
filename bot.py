#!/usr/bin/env python3
"""
bot_single_fixed.py
Single-file Telegram video processing bot (fixed & cleaned).
- Pyrogram (async)
- Motor (MongoDB)
- Async ffmpeg with -progress pipe:1 parsing
- Inline UI, per-task logs, FastAPI API, shortener, auth, admin
"""

import os
import sys
import asyncio
import json
import shlex
import logging
import uuid
from typing import Optional, List, Dict, Any, Callable

# Third-party libs
try:
    from dotenv import load_dotenv
    from pyrogram import Client, filters
    from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton, Message
    from motor.motor_asyncio import AsyncIOMotorClient
    import aiohttp
    from fastapi import FastAPI, Header, HTTPException
    import uvicorn
except Exception as e:
    print("Missing dependencies. Install with: pip install -r requirements.txt")
    raise

# ---------------------------
# Load env
# ---------------------------
load_dotenv()

API_ID = int(os.getenv("API_ID", "0"))
API_HASH = os.getenv("API_HASH", "")
BOT_TOKEN = os.getenv("BOT_TOKEN", "")
SESSION_NAME = os.getenv("SESSION_NAME", "video_processor_bot_single")

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
DB_NAME = os.getenv("DB_NAME", "video_bot_single")

# Admins, log channel, auth group
ADMINS = [int(x) for x in os.getenv("ADMINS", "").split(",") if x.strip().isdigit()]
LOG_CHANNEL = int(os.getenv("LOG_CHANNEL")) if os.getenv("LOG_CHANNEL") else None
AUTH_GROUP = int(os.getenv("AUTH_GROUP")) if os.getenv("AUTH_GROUP") else None

# Shortener config
SHORTENER_API = os.getenv("SHORTENER_API", "")
SHORTENER_TOKEN = os.getenv("SHORTENER_TOKEN", "")
SHORTENER_TIMEOUT = int(os.getenv("SHORTENER_TIMEOUT", "20"))

# Limits & concurrency
MAX_CONCURRENT_TASKS = int(os.getenv("MAX_CONCURRENT_TASKS", "2"))
MAX_FILE_SIZE = int(os.getenv("MAX_FILE_SIZE", str(2 * 1024 * 1024 * 1024)))  # default 2GB

# App ports
API_PORT = int(os.getenv("PORT", "8000"))

# Basic checks
if not (API_ID and API_HASH and BOT_TOKEN):
    print("Set API_ID, API_HASH and BOT_TOKEN in environment.")
    sys.exit(1)

# ---------------------------
# Logging & directories
# ---------------------------
BASE_DIR = os.path.dirname(__file__)
LOGS_DIR = os.path.join(BASE_DIR, "logs")
DOWNLOADS_DIR = os.path.join(BASE_DIR, "downloads")
os.makedirs(LOGS_DIR, exist_ok=True)
os.makedirs(DOWNLOADS_DIR, exist_ok=True)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("video-bot-single")


def get_task_logger(task_id: str):
    name = f"task-{task_id}"
    p = os.path.join(LOGS_DIR, f"{task_id}.log")
    tl = logging.getLogger(name)
    if not tl.handlers:
        tl.setLevel(logging.INFO)
        fh = logging.FileHandler(p)
        fh.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
        tl.addHandler(fh)
    return tl

# ---------------------------
# Database (Motor)
# ---------------------------
mongo = AsyncIOMotorClient(MONGO_URI)
db = mongo[DB_NAME]


async def ensure_bot_config():
    doc = await db.bot_config.find_one({"_id": "bot"})
    if not doc:
        default = {
            "_id": "bot",
            "max_concurrent_tasks": MAX_CONCURRENT_TASKS,
            "max_file_size": MAX_FILE_SIZE,
            "shortener_api": SHORTENER_API,
            "shortener_token": SHORTENER_TOKEN
        }
        await db.bot_config.insert_one(default)
        return default
    return doc


async def get_user_config(user_id: int) -> dict:
    doc = await db.user_config.find_one({"user_id": int(user_id)})
    if not doc:
        default = {
            "user_id": int(user_id),
            "default_quality": 720,
            "hardsub": False,
            "audio_keep": [],  # empty => keep all
            "thumbnail_file_id": None
        }
        await db.user_config.insert_one(default)
        return default
    return doc


async def set_user_config(user_id: int, data: dict):
    await db.user_config.update_one({"user_id": int(user_id)}, {"$set": data}, upsert=True)


async def add_api_token(token: str, note: str = ""):
    await db.api_tokens.insert_one({"token": token, "note": note})


async def is_valid_api_token(token: str) -> bool:
    res = await db.api_tokens.find_one({"token": token})
    return res is not None


async def create_task(task: dict):
    await db.tasks.insert_one(task)


async def update_task(task_id: str, data: dict):
    await db.tasks.update_one({"_id": task_id}, {"$set": data})


async def get_stats() -> dict:
    users = await db.user_config.count_documents({})
    tasks_total = await db.tasks.count_documents({})
    active = await db.tasks.count_documents({"status": "running"})
    return {"users": users, "tasks_total": tasks_total, "active_tasks": active}

# ---------------------------
# Pyrogram client
# ---------------------------
app = Client(SESSION_NAME, api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)

# ---------------------------
# Auth filter: ADMINS or AUTH_GROUP
# ---------------------------
def auth_filter():
    async def f(_, __, message: Message):
        # allow admins
        if message.from_user and message.from_user.id in ADMINS:
            return True
        # if chat is the authorized group
        if message.chat and AUTH_GROUP and message.chat.id == AUTH_GROUP:
            return True
        return False
    return filters.create(f)

# ---------------------------
# Shortener utility with timeout
# ---------------------------
async def shorten_url(url: str, api: Optional[str] = None, token: Optional[str] = None, timeout: int = SHORTENER_TIMEOUT) -> str:
    api = api or (await ensure_bot_config()).get("shortener_api") or SHORTENER_API
    token = token or (await ensure_bot_config()).get("shortener_token") or SHORTENER_TOKEN
    if not api:
        return url
    try:
        async with aiohttp.ClientSession() as sess:
            # Example: shrtco.de style (GET query param `url`)
            if "shrtco.de" in api:
                call = f"{api}{url}"
                async with sess.get(call, timeout=timeout) as resp:
                    j = await resp.json()
                    if j.get("ok"):
                        return j["result"]["full_short_link"]
                    return url
            # Generic: if api endswith '=' treat as GET param
            if api.endswith("="):
                call = f"{api}{url}"
                async with sess.get(call, timeout=timeout) as resp:
                    text = await resp.text()
                    return text.strip() or url
            # Generic POST JSON
            headers = {}
            if token:
                headers["Authorization"] = f"Bearer {token}"
            async with sess.post(api, json={"url": url}, headers=headers, timeout=timeout) as resp:
                try:
                    j = await resp.json()
                    if isinstance(j, dict):
                        for key in ("result", "shorturl", "short_url", "link", "url"):
                            if key in j:
                                v = j[key]
                                if isinstance(v, dict) and "url" in v:
                                    return v["url"]
                                if isinstance(v, str):
                                    return v
                    return url
                except Exception:
                    text = await resp.text()
                    return text.strip() or url
    except asyncio.TimeoutError:
        return url
    except Exception as e:
        logger.exception("Shortener error: %s", e)
        return url

# ---------------------------
# FFmpeg helpers
# ---------------------------
QUALITY_MAP = {360: 360, 480: 480, 720: 720, 1080: 1080}


async def ffprobe_metadata(path: str) -> dict:
    try:
        proc = await asyncio.create_subprocess_exec(
            "ffprobe", "-v", "quiet", "-print_format", "json", "-show_format", "-show_streams", path,
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
        )
        out, err = await proc.communicate()
        if not out:
            return {}
        return json.loads(out)
    except Exception:
        return {}


async def build_ffmpeg_cmd(input_path: str, output_path: str, quality: int = 720,
                           hardsub: bool = False, subtitle_path: Optional[str] = None,
                           keep_audio: Optional[List[str]] = None) -> List[str]:
    keep_audio = keep_audio or []
    meta = await ffprobe_metadata(input_path)
    audio_streams = [s for s in meta.get("streams", []) if s.get("codec_type") == "audio"]
    video_streams = [s for s in meta.get("streams", []) if s.get("codec_type") == "video"]
    # Map video stream (first)
    maps = []
    if video_streams:
        vid_idx = video_streams[0].get("index", 0)
    else:
        vid_idx = 0
    maps += ["-map", f"0:{vid_idx}"]
    # Map audio streams based on keep_audio (by language) or keep all
    if not keep_audio:
        for s in audio_streams:
            maps += ["-map", f"0:{s.get('index')}"]
    else:
        want = [k.lower() for k in keep_audio]
        for s in audio_streams:
            tags = s.get("tags") or {}
            lang = tags.get("language", "").lower()
            if lang and any(w in lang for w in want):
                maps += ["-map", f"0:{s.get('index')}"]
    target_h = QUALITY_MAP.get(int(quality), 720)
    vf = f"scale=-2:{target_h}"
    if hardsub and subtitle_path and os.path.exists(subtitle_path):
        vf = vf + f",subtitles={subtitle_path}"
    cmd = ["ffmpeg", "-y", "-hide_banner", "-i", input_path] + maps + [
        "-c:v", "libx264", "-preset", "veryfast", "-crf", "23",
        "-vf", vf,
        "-c:a", "aac", "-b:a", "128k",
        "-progress", "pipe:1", "-nostats", output_path
    ]
    return cmd


class FFmpegProgressParser:
    def __init__(self, duration: float, on_update: Optional[Callable[[float, dict], Any]] = None):
        self.on_update = on_update
        self.duration = duration or 0.0
        self.data = {}

    async def feed(self, reader: asyncio.StreamReader):
        while True:
            line = await reader.readline()
            if not line:
                break
            text = line.decode(errors="ignore").strip()
            if not text:
                continue
            if "=" in text:
                k, v = text.split("=", 1)
                self.data[k.strip()] = v.strip()
            if self.data.get("progress") == "end":
                if self.on_update:
                    await maybe_async(self.on_update, 100.0, self.data.copy())
                break
            out_ms = self.data.get("out_time_ms")
            if out_ms and self.duration:
                try:
                    percent = min(100.0, (int(out_ms) / 1000.0) / self.duration * 100.0)
                    if self.on_update:
                        await maybe_async(self.on_update, percent, self.data.copy())
                except Exception:
                    pass


async def maybe_async(func, *args, **kwargs):
    res = func(*args, **kwargs)
    if asyncio.iscoroutine(res):
        return await res
    return res


async def run_ffmpeg_task(task_id: str, input_path: str, output_path: str, quality: int, hardsub: bool, subtitle_path: Optional[str],
                          keep_audio: List[str], on_progress: Optional[Callable[[float, dict], Any]] = None) -> int:
    tl = get_task_logger(task_id)
    tl.info("Running ffmpeg task: in=%s out=%s q=%s hardsub=%s keep_audio=%s", input_path, output_path, quality, hardsub, keep_audio)
    meta = await ffprobe_metadata(input_path)
    duration = 0.0
    try:
        duration = float(meta.get("format", {}).get("duration") or 0.0)
    except Exception:
        duration = 0.0
    cmd = await build_ffmpeg_cmd(input_path, output_path, quality, hardsub, subtitle_path, keep_audio)
    tl.info("CMD: %s", " ".join(shlex.quote(x) for x in cmd))
    try:
        proc = await asyncio.create_subprocess_exec(*cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
    except FileNotFoundError:
        tl.exception("ffmpeg not found")
        return 127
    parser = FFmpegProgressParser(duration=duration, on_update=on_progress)
    feed_task = asyncio.create_task(parser.feed(proc.stdout))
    async def drain(reader):
        try:
            while True:
                d = await reader.read(1024)
                if not d:
                    break
        except Exception:
            pass
    drain_task = asyncio.create_task(drain(proc.stderr))
    rc = await proc.wait()
    await feed_task
    await drain_task
    tl.info("ffmpeg exit code: %s", rc)
    return rc

# ---------------------------
# UI & Handlers
# ---------------------------
def user_settings_kb(conf: dict):
    q = conf.get("default_quality", 720)
    h = conf.get("hardsub", False)
    audio = conf.get("audio_keep", [])
    kb = [
        [InlineKeyboardButton(f"Quality: {q}p", callback_data="u_set_quality")],
        [InlineKeyboardButton(f"Hardsub: {'ON' if h else 'OFF'}", callback_data="u_toggle_hardsub")],
        [InlineKeyboardButton(f"Audio keep: {','.join(audio) if audio else 'ALL'}", callback_data="u_set_audio")],
        [InlineKeyboardButton("Save thumbnail", callback_data="u_save_thumb")],
        [InlineKeyboardButton("Reset", callback_data="u_reset")]
    ]
    return InlineKeyboardMarkup(kb)


def admin_kb():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("Max concurrent tasks", callback_data="a_max_tasks")],
        [InlineKeyboardButton("Max file size", callback_data="a_max_size")],
        [InlineKeyboardButton("View stats", callback_data="a_stats")],
        [InlineKeyboardButton("Manage API tokens", callback_data="a_tokens")]
    ])


@app.on_message(filters.command("start") & (filters.private | auth_filter()))
async def start_cmd(client, message: Message):
    await ensure_bot_config()
    await message.reply_text("Hello — send a video (private or in authorized group). Use /usettings for your defaults.")


@app.on_message(filters.command("usettings") & filters.private)
async def cmd_usettings(client, message: Message):
    conf = await get_user_config(message.from_user.id)
    await message.reply_text("Your settings (inline):", reply_markup=user_settings_kb(conf))


# Save pending action (like save thumbnail)
async def set_pending(user_id: int, action: Optional[str]):
    await db.user_pending.update_one({"user_id": int(user_id)}, {"$set": {"action": action}}, upsert=True)


async def get_pending(user_id: int) -> Optional[str]:
    doc = await db.user_pending.find_one({"user_id": int(user_id)})
    return doc.get("action") if doc else None


@app.on_message(filters.photo & filters.private)
async def handle_photo(client, message: Message):
    pending = await get_pending(message.from_user.id)
    if pending == "save_thumb":
        # save largest photo file_id
        file_id = message.photo.file_id
        await set_user_config(message.from_user.id, {"thumbnail_file_id": file_id})
        await set_pending(message.from_user.id, None)
        await message.reply_text("Thumbnail saved.")
    else:
        pass


# Media handler accepts both document and video (authorize)
@app.on_message((filters.video | filters.document) & (filters.private | auth_filter()))
async def media_handler(client, message: Message):
    bot_conf = await ensure_bot_config()
    max_size = bot_conf.get("max_file_size", MAX_FILE_SIZE)
    doc = message.document or message.video
    size = getattr(doc, "file_size", 0) or 0
    if size and size > max_size:
        await message.reply_text(f"File too large. Limit: {max_size} bytes")
        return

    # create task record
    task_id = str(uuid.uuid4())
    rec = {
        "_id": task_id,
        "user_id": message.from_user.id if message.from_user else None,
        "chat_id": message.chat.id,
        "msg_id": message.message_id,
        "file_id": doc.file_id,
        "file_size": size,
        "status": "options",
        "created_at": int(asyncio.get_event_loop().time()),
        "bot_msg_id": None
    }
    await create_task(rec)

    # send a placeholder message we will edit for progress
    bot_msg = await app.send_message(message.chat.id, "Choose quality and options (inline buttons will control the task).")
    # save bot message id in task
    await update_task(task_id, {"bot_msg_id": bot_msg.message_id})

    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("360p", callback_data=f"q:{task_id}:360"), InlineKeyboardButton("480p", callback_data=f"q:{task_id}:480")],
        [InlineKeyboardButton("720p", callback_data=f"q:{task_id}:720"), InlineKeyboardButton("1080p", callback_data=f"q:{task_id}:1080")],
        [InlineKeyboardButton("Toggle Hardsub (user default)", callback_data=f"hardsub_toggle:{task_id}")],
        [InlineKeyboardButton("Keep ENG audio", callback_data=f"audio:{task_id}:eng"), InlineKeyboardButton("Keep ALL", callback_data=f"audio:{task_id}:all")],
        [InlineKeyboardButton("Start ▶", callback_data=f"start:{task_id}")]
    ])
    await app.edit_message_text(message.chat.id, bot_msg.message_id, "Choose quality and options (all inline):", reply_markup=kb)


# Inline callback router
@app.on_callback_query()
async def on_cb(client, cq):
    data = cq.data or ""
    # Quality pick
    if data.startswith("q:"):
        _, task_id, q = data.split(":", 2)
        await update_task(task_id, {"selected_quality": int(q)})
        await cq.answer(f"Quality set to {q}p")
        return
    # Toggle per-task hardsub
    if data.startswith("hardsub_toggle:"):
        _, task_id = data.split(":", 1)
        cur = await db.tasks.find_one({"_id": task_id})
        new = not cur.get("hardsub_user", False)
        await update_task(task_id, {"hardsub_user": new})
        await cq.answer(f"Hardsub toggled to {'ON' if new else 'OFF'}")
        return
    # Audio selection
    if data.startswith("audio:"):
        _, task_id, choice = data.split(":", 2)
        if choice == "all":
            await update_task(task_id, {"audio_keep": []})
            await cq.answer("Will keep all audio tracks")
        else:
            await update_task(task_id, {"audio_keep": [choice]})
            await cq.answer(f"Will keep audio: {choice}")
        return
    # Start processing
    if data.startswith("start:"):
        _, task_id = data.split(":", 1)
        task = await db.tasks.find_one({"_id": task_id})
        if not task:
            await cq.answer("Task not found", show_alert=True)
            return
        await cq.answer("Queued for processing")
        # update placeholder text
        bot_msg_id = task.get("bot_msg_id")
        if bot_msg_id:
            try:
                await app.edit_message_text(task["chat_id"], bot_msg_id, "Queued — starting processing now. Progress will update here.")
            except Exception:
                pass
        # launch background
        asyncio.create_task(process_task(task_id))
        return

    # user settings callbacks (prefixed u_)
    if data == "u_set_quality":
        uid = cq.from_user.id
        conf = await get_user_config(uid)
        opts = [360, 480, 720, 1080]
        cur = conf.get("default_quality", 720)
        try:
            nxt = opts[(opts.index(cur) + 1) % len(opts)]
        except Exception:
            nxt = 720
        await set_user_config(uid, {"default_quality": nxt})
        await cq.answer(f"Default quality set to {nxt}p")
        await cq.message.edit_reply_markup(reply_markup=user_settings_kb(await get_user_config(uid)))
        return
    if data == "u_toggle_hardsub":
        uid = cq.from_user.id
        conf = await get_user_config(uid)
        n = not conf.get("hardsub", False)
        await set_user_config(uid, {"hardsub": n})
        await cq.answer("Hardsub toggled")
        await cq.message.edit_reply_markup(reply_markup=user_settings_kb(await get_user_config(uid)))
        return
    if data == "u_set_audio":
        uid = cq.from_user.id
        conf = await get_user_config(uid)
        if conf.get("audio_keep"):
            await set_user_config(uid, {"audio_keep": []})
        else:
            await set_user_config(uid, {"audio_keep": ["eng"]})
        await cq.answer("Audio preference updated")
        await cq.message.edit_reply_markup(reply_markup=user_settings_kb(await get_user_config(uid)))
        return
    if data == "u_save_thumb":
        await set_pending(cq.from_user.id, "save_thumb")
        await cq.answer("Send a photo now to save as thumbnail")
        await cq.message.edit_text("Now send a photo to save as your thumbnail.")
        return
    if data == "u_reset":
        await set_user_config(cq.from_user.id, {"default_quality": 720, "hardsub": False, "audio_keep": [], "thumbnail_file_id": None})
        await cq.answer("Settings reset")
        await cq.message.edit_reply_markup(reply_markup=user_settings_kb(await get_user_config(cq.from_user.id)))
        return

    # admin KB callbacks prefixed a_
    if data == "a_max_tasks":
        if cq.from_user.id not in ADMINS:
            await cq.answer("Not authorized", show_alert=True)
            return
        await cq.message.edit_text("Send a message with the new integer value for max concurrent tasks.")
        await cq.answer("Send number in chat")
        return
    if data == "a_max_size":
        if cq.from_user.id not in ADMINS:
            await cq.answer("Not authorized", show_alert=True)
            return
        await cq.message.edit_text("Send a message with the new max file size in bytes.")
        await cq.answer("Send number in chat")
        return
    if data == "a_stats":
        if cq.from_user.id not in ADMINS:
            await cq.answer("Not authorized", show_alert=True)
            return
        s = await get_stats()
        await cq.message.edit_text(f"Users: {s['users']}\nTasks total: {s['tasks_total']}\nActive: {s['active_tasks']}")
        await cq.answer("Stats shown")
        return
    if data == "a_tokens":
        if cq.from_user.id not in ADMINS:
            await cq.answer("Not authorized", show_alert=True)
            return
        await cq.message.edit_text("Reply with a token string to add to API tokens.")
        await cq.answer("Send token")
        return

    await cq.answer()

# Admin text handler for numeric settings and adding tokens
@app.on_message(filters.private & filters.text & filters.user(ADMINS))
async def admin_messages(client, message: Message):
    text = message.text.strip()
    if len(text) > 30 and " " not in text:
        # add as token
        try:
            await add_api_token(text, note=f"added_by:{message.from_user.id}")
            await message.reply_text("API token added.")
            if LOG_CHANNEL:
                await app.send_message(LOG_CHANNEL, f"Admin {message.from_user.id} added API token.")
            return
        except Exception as e:
            await message.reply_text(f"Failed to add token: {e}")
            return
    try:
        n = int(text)
        await db.bot_config.update_one({"_id": "bot"}, {"$set": {"max_concurrent_tasks": n}}, upsert=True)
        await message.reply_text(f"Updated max_concurrent_tasks to {n}")
        if LOG_CHANNEL:
            await app.send_message(LOG_CHANNEL, f"Admin {message.from_user.id} set max_concurrent_tasks={n}")
        return
    except ValueError:
        await message.reply_text("Unknown admin input. Send a numeric value to set max tasks or a token string to add.")
        return

# ---------------------------
# Processing controller
# ---------------------------
concurrency_sem = asyncio.Semaphore(MAX_CONCURRENT_TASKS)

async def process_task(task_id: str):
    tl = get_task_logger(task_id)
    tl.info("Start processing %s", task_id)
    task = await db.tasks.find_one({"_id": task_id})
    if not task:
        tl.error("Task not found")
        return
    await update_task(task_id, {"status": "running"})
    uid = task.get("user_id")
    user_conf = await get_user_config(uid) if uid else {}
    quality = task.get("selected_quality") or user_conf.get("default_quality", 720)
    hardsub_user = task.get("hardsub_user")
    if hardsub_user is None:
        hardsub = user_conf.get("hardsub", False)
    else:
        hardsub = hardsub_user
    keep_audio = task.get("audio_keep")
    if keep_audio is None:
        keep_audio = user_conf.get("audio_keep", [])
    chat_id = task.get("chat_id")
    bot_msg_id = task.get("bot_msg_id")
    file_id = task.get("file_id")
    input_path = os.path.join(DOWNLOADS_DIR, f"{task_id}_in")
    output_path = os.path.join(DOWNLOADS_DIR, f"{task_id}_out.mp4")
    try:
        await app.download_media(file_id, file_name=input_path)
    except Exception as e:
        tl.exception("Download failed: %s", e)
        await app.send_message(chat_id, f"Download failed: {e}")
        await update_task(task_id, {"status": "failed", "error": str(e)})
        return
    subtitle_path = None
    async def on_progress(percent: float, meta: dict):
        try:
            bars = 12
            filled = int(percent / 100.0 * bars)
            bar = "[" + "█" * filled + "-" * (bars - filled) + "]"
            text = f"Processing: {percent:.1f}%\n{bar}"
            if bot_msg_id:
                try:
                    await app.edit_message_text(chat_id, bot_msg_id, text)
                except Exception:
                    pass
            if percent and percent % 5 < 0.5:
                await update_task(task_id, {"last_percent": percent})
        except Exception:
            pass
    bot_conf = await ensure_bot_config()
    try:
        async with concurrency_sem:
            rc = await run_ffmpeg_task(task_id, input_path, output_path, int(quality), bool(hardsub), subtitle_path, keep_audio, on_progress)
    except Exception as e:
        tl.exception("Processing exception: %s", e)
        await app.send_message(chat_id, f"Processing error: {e}")
        await update_task(task_id, {"status": "failed", "error": str(e)})
        try:
            os.remove(input_path)
        except Exception:
            pass
        return
    if rc != 0:
        tl.error("ffmpeg returned non-zero: %s", rc)
        await app.send_message(chat_id, "Processing failed. Check logs.")
        await update_task(task_id, {"status": "failed", "exit_code": rc})
        try:
            os.remove(input_path)
            if os.path.exists(output_path):
                os.remove(output_path)
        except Exception:
            pass
        return
    try:
        sent = await app.send_document(chat_id, output_path)
        file_url = f"https://t.me/{(await app.get_me()).username}/{sent.message_id}"
        short = await shorten_url(file_url, timeout=SHORTENER_TIMEOUT)
        await app.send_message(chat_id, f"Done! Download: {short}")
        await update_task(task_id, {"status": "done", "file_message_id": sent.message_id})
        if LOG_CHANNEL:
            await app.send_message(LOG_CHANNEL, f"Task {task_id} completed by user {uid}")
    except Exception as e:
        tl.exception("Upload failed: %s", e)
        await app.send_message(chat_id, f"Upload failed: {e}")
        await update_task(task_id, {"status": "failed", "error": str(e)})
    finally:
        try:
            os.remove(input_path)
        except Exception:
            pass
        try:
            os.remove(output_path)
        except Exception:
            pass
    tl.info("Task finished")

# ---------------------------
# FastAPI token-protected API
# ---------------------------
api_app = FastAPI(title="Video Bot API")

async def require_token(x_api_key: Optional[str] = Header(None)):
    if not x_api_key:
        raise HTTPException(status_code=401, detail="Missing X-API-KEY")
    if not await is_valid_api_token(x_api_key):
        raise HTTPException(status_code=403, detail="Invalid token")
    return True

@api_app.get("/health")
async def health():
    return {"ok": True}

@api_app.get("/status")
async def status(x_api_key: Optional[str] = Header(None)):
    await require_token(x_api_key)
    return {"ok": True, "stats": await get_stats()}

# ---------------------------
# Start both bot and API
# ---------------------------
async def _main():
    await ensure_bot_config()
    config_uvicorn = uvicorn.Config(api_app, host="0.0.0.0", port=API_PORT, log_level="info")
    server = uvicorn.Server(config_uvicorn)

    # Start Pyrogram client
    await app.start()
    logger.info("✅ Bot started; API server running on port %s", API_PORT)

    # Run FastAPI in background
    api_task = asyncio.create_task(server.serve())

    try:
        await asyncio.Event().wait()  # keep alive until interrupted
    except (KeyboardInterrupt, asyncio.CancelledError):
        logger.info("Shutting down gracefully...")
    finally:
        # stop bot
        await app.stop()
        # stop API server
        api_task.cancel()
        try:
            await api_task
        except asyncio.CancelledError:
            pass
        logger.info("Bot & API stopped")
