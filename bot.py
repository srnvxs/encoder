import os
import sys
import asyncio
import json
import shlex
import logging
import uuid
from typing import Optional, Callable, Dict, Any, List

# third-party libs
try:
    from dotenv import load_dotenv
    from motor.motor_asyncio import AsyncIOMotorClient
    from pyrogram import Client, filters
    from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton
    import aiohttp
    from fastapi import FastAPI, Header, HTTPException
    import uvicorn
except Exception as e:
    print("Missing dependencies. Install with: pip install -r requirements.txt")
    raise

# load env
load_dotenv()

# -----------------------------
# Configuration (env variables)
# -----------------------------
API_ID = int(os.getenv("API_ID", "0"))
API_HASH = os.getenv("API_HASH", "")
BOT_TOKEN = os.getenv("BOT_TOKEN", "")
SESSION_NAME = os.getenv("SESSION_NAME", "video_processor_bot_single")
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
DB_NAME = os.getenv("DB_NAME", "video_bot_single")
SHORTENER_API = os.getenv("SHORTENER_API", "")  # example: https://api.shrtco.de/v2/shorten?url=
SHORTENER_TOKEN = os.getenv("SHORTENER_TOKEN", "")
MAX_CONCURRENT_TASKS = int(os.getenv("MAX_CONCURRENT_TASKS", "2"))
MAX_FILE_SIZE = int(os.getenv("MAX_FILE_SIZE", str(2 * 1024 * 1024 * 1024)))  # 2GB default
PORT = int(os.getenv("PORT", "8000"))
ADMINS = [int(x) for x in os.getenv("ADMINS", "").split(",") if x.strip().isdigit()]

if not (API_ID and API_HASH and BOT_TOKEN):
    print("You must set API_ID, API_HASH and BOT_TOKEN in environment.")
    # Do not exit automatically in contexts where user may want to read file.
    # But we'll exit here because bot cannot run without credentials.
    sys.exit(1)

# -----------------------------
# Logging setup (global)
# -----------------------------
LOGS_DIR = os.path.join(os.path.dirname(__file__), "logs")
os.makedirs(LOGS_DIR, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout)
    ],
)
logger = logging.getLogger("video_bot_single")


def get_task_logger(task_id: str):
    name = f"task-{task_id}"
    log_path = os.path.join(LOGS_DIR, f"{task_id}.log")
    tl = logging.getLogger(name)
    if not tl.handlers:
        tl.setLevel(logging.INFO)
        fh = logging.FileHandler(log_path)
        fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
        fh.setFormatter(fmt)
        tl.addHandler(fh)
    return tl


# -----------------------------
# MongoDB (motor)
# -----------------------------
client = AsyncIOMotorClient(MONGO_URI)
db = client[DB_NAME]


async def get_user_config(user_id: int) -> dict:
    doc = await db.user_config.find_one({"user_id": int(user_id)})
    if not doc:
        default = {
            "user_id": int(user_id),
            "default_quality": 720,
            "hardsub": False,
            "audio_keep": [],  # e.g. ["eng"]
            "thumbnail_file_id": None
        }
        await db.user_config.insert_one(default)
        return default
    return doc


async def set_user_config(user_id: int, data: dict):
    await db.user_config.update_one({"user_id": int(user_id)}, {"$set": data}, upsert=True)


async def get_bot_config() -> dict:
    doc = await db.bot_config.find_one({"_id": "bot"})
    if not doc:
        default = {
            "_id": "bot",
            "max_concurrent_tasks": MAX_CONCURRENT_TASKS,
            "max_file_size": MAX_FILE_SIZE,
            "shortener_api": SHORTENER_API,
            "shortener_token": SHORTENER_TOKEN,
        }
        await db.bot_config.insert_one(default)
        return default
    return doc


async def set_bot_config(data: dict):
    await db.bot_config.update_one({"_id": "bot"}, {"$set": data}, upsert=True)


async def add_api_token(token: str, note: str = ""):
    await db.api_tokens.insert_one({"token": token, "note": note})


async def is_valid_api_token(token: str) -> bool:
    res = await db.api_tokens.find_one({"token": token})
    return res is not None


async def create_task_record(task: dict):
    return await db.tasks.insert_one(task)


async def update_task_record(_id, data: dict):
    return await db.tasks.update_one({"_id": _id}, {"$set": data})


async def get_stats() -> dict:
    users = await db.user_config.count_documents({})
    tasks_total = await db.tasks.count_documents({})
    active = await db.tasks.count_documents({"status": "running"})
    return {"users": users, "tasks_total": tasks_total, "active_tasks": active}


# -----------------------------
# Utilities: shortener
# -----------------------------
async def shorten_url(url: str, api: Optional[str] = None, token: Optional[str] = None) -> str:
    api = api or (await get_bot_config()).get("shortener_api") or SHORTENER_API
    token = token or (await get_bot_config()).get("shortener_token") or SHORTENER_TOKEN
    if not api:
        return url
    # shrtco example: https://api.shrtco.de/v2/shorten?url=
    try:
        async with aiohttp.ClientSession() as sess:
            if "shrtco.de" in api:
                call = f"{api}{url}"
                async with sess.get(call) as resp:
                    j = await resp.json()
                    if j.get("ok"):
                        return j["result"]["full_short_link"]
                    return url
            else:
                # generic GET append
                if api.endswith("="):
                    call = f"{api}{url}"
                    async with sess.get(call) as resp:
                        txt = await resp.text()
                        return txt.strip() or url
                # fallback: return original
                return url
    except Exception as e:
        logger.exception("Shortener error: %s", e)
        return url


# -----------------------------
# Utilities: FFmpeg probe & build
# -----------------------------
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
    except Exception as e:
        logger.exception("ffprobe failed: %s", e)
        return {}


async def build_ffmpeg_cmd(input_path: str, output_path: str, quality: int = 720,
                           hardsub: bool = False, subtitle_path: Optional[str] = None,
                           keep_audio: Optional[List[str]] = None) -> List[str]:
    keep_audio = keep_audio or []
    meta = await ffprobe_metadata(input_path)
    audio_streams = [s for s in meta.get("streams", []) if s.get("codec_type") == "audio"]
    # Build mapping args
    map_args = []
    # always map first video stream
    # find first video stream index
    video_streams = [s for s in meta.get("streams", []) if s.get("codec_type") == "video"]
    if video_streams:
        vid_idx = video_streams[0].get("index", 0)
    else:
        vid_idx = 0
    # map video
    map_args += ["-map", f"0:{vid_idx}"]
    # choose audio streams to keep
    for s in audio_streams:
        idx = s.get("index")
        tags = s.get("tags") or {}
        lang = tags.get("language", "").lower()
        # if keep_audio is empty -> keep all
        if not keep_audio:
            map_args += ["-map", f"0:{idx}"]
        else:
            for k in keep_audio:
                if k and lang and k.lower() in lang:
                    map_args += ["-map", f"0:{idx}"]
                    break
    # video scaling & filters
    target_h = QUALITY_MAP.get(int(quality), 720)
    vf = f"scale=-2:{target_h}"
    if hardsub and subtitle_path and os.path.exists(subtitle_path):
        # subtitle filter expects proper escaping sometimes.
        # Use subtitles=subtitle_path
        vf = vf + f",subtitles={subtitle_path}"
    # use x264, aac
    cmd = ["ffmpeg", "-y", "-hide_banner", "-i", input_path] + map_args + [
        "-c:v", "libx264", "-preset", "veryfast", "-crf", "23",
        "-vf", vf,
        "-c:a", "aac", "-b:a", "128k",
        "-progress", "pipe:1", "-nostats", output_path
    ]
    return cmd


# -----------------------------
# Utilities: parse ffmpeg progress
# -----------------------------
class FFmpegProgressParser:
    """
    Parse ffmpeg -progress pipe:1 lines and call on_update(percent, data)
    Heuristic: uses duration from ffprobe (seconds) and out_time_ms to estimate percent.
    """

    def __init__(self, duration: Optional[float], on_update: Optional[Callable[[float, dict], asyncio.Future]] = None):
        self.on_update = on_update
        self._data: Dict[str, str] = {}
        self.duration = duration or 0.0

    async def feed(self, reader: asyncio.StreamReader):
        # read lines
        while True:
            line = await reader.readline()
            if not line:
                break
            text = line.decode(errors="ignore").strip()
            if not text:
                continue
            if "=" in text:
                k, v = text.split("=", 1)
                self._data[k.strip()] = v.strip()
            # If progress=end -> 100%
            if self._data.get("progress") == "end":
                if self.on_update:
                    try:
                        await self.on_update(100.0, self._data.copy())
                    except Exception:
                        pass
                break
            # compute percent
            out_time_ms = self._data.get("out_time_ms")
            if out_time_ms and self.duration:
                try:
                    out_ms = int(out_time_ms)
                    percent = min(100.0, (out_ms / 1000.0) / self.duration * 100.0)
                    if self.on_update:
                        try:
                            await self.on_update(percent, self._data.copy())
                        except Exception:
                            pass
                except Exception:
                    pass


# -----------------------------
# Run ffmpeg task
# -----------------------------
async def run_ffmpeg_task(task_id: str, input_path: str, output_path: str,
                          quality: int, hardsub: bool, subtitle_path: Optional[str],
                          keep_audio: List[str],
                          on_progress: Optional[Callable[[float, dict], asyncio.Future]] = None) -> int:
    tlog = get_task_logger(task_id)
    tlog.info("Starting ffmpeg task: in=%s out=%s q=%s hardsub=%s keep_audio=%s",
              input_path, output_path, quality, hardsub, keep_audio)
    # get duration for percent estimation
    meta = await ffprobe_metadata(input_path)
    duration = 0.0
    try:
        duration = float(meta.get("format", {}).get("duration") or 0.0)
    except Exception:
        duration = 0.0
    cmd = await build_ffmpeg_cmd(input_path, output_path, quality, hardsub, subtitle_path, keep_audio)
    tlog.info("FFmpeg CMD: %s", " ".join(shlex.quote(x) for x in cmd))
    try:
        proc = await asyncio.create_subprocess_exec(*cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
    except FileNotFoundError:
        tlog.exception("ffmpeg binary not found. Make sure ffmpeg is installed.")
        return 127
    # parse progress from stdout
    parser = FFmpegProgressParser(duration=duration, on_update=on_progress)
    # Note: ffmpeg emits progress to stdout when using -progress pipe:1
    feed_task = asyncio.create_task(parser.feed(proc.stdout))
    # also consume stderr so process doesn't block
    async def drain_stream(reader: asyncio.StreamReader):
        try:
            while True:
                d = await reader.read(1024)
                if not d:
                    break
        except Exception:
            pass
    drain_task = asyncio.create_task(drain_stream(proc.stderr))
    rc = await proc.wait()
    await feed_task
    await drain_task
    tlog.info("ffmpeg exit code: %s", rc)
    return rc


# -----------------------------
# Pyrogram client & handlers
# -----------------------------
app = Client(SESSION_NAME, api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)


def settings_keyboard(user_conf: dict):
    q = user_conf.get("default_quality", 720)
    h = user_conf.get("hardsub", False)
    audio_keep = user_conf.get("audio_keep", [])
    kb = [
        [InlineKeyboardButton(f"Quality: {q}p", callback_data="set_quality")],
        [InlineKeyboardButton(f"Hardsub: {'ON' if h else 'OFF'}", callback_data="toggle_hardsub")],
        [InlineKeyboardButton(f"Audio keep: {','.join(audio_keep) if audio_keep else 'ALL'}", callback_data="set_audio")],
        [InlineKeyboardButton("Save thumbnail", callback_data="save_thumb")],
        [InlineKeyboardButton("Reset settings", callback_data="reset_settings")]
    ]
    return InlineKeyboardMarkup(kb)


# helper: show basic start text
@app.on_message(filters.private & filters.command("start"))
async def cmd_start(client, message):
    await message.reply_text("Hello! Send a video (or a document) and I'll show inline options for compress/encode.\nUse /usettings to configure defaults (inline buttons).")


@app.on_message(filters.private & filters.command("usettings"))
async def cmd_usettings(client, message):
    user_conf = await get_user_config(message.from_user.id)
    await message.reply_text("Your settings (use inline buttons):", reply_markup=settings_keyboard(user_conf))


# When receiving a photo after 'save_thumb' instruction, we need to handle saving it.
# For simplicity: when user sends photo and their last settings message asked to save thumbnail we handle it.
# We'll implement a simple approach: any photo sent after pressing 'Save thumbnail' inline will be used. To track that
# we will set a field user.pending_action = 'await_thumb' in DB.
async def set_pending_action(user_id: int, action: Optional[str]):
    await db.user_pending.update_one({"user_id": int(user_id)}, {"$set": {"action": action}}, upsert=True)


async def get_pending_action(user_id: int):
    doc = await db.user_pending.find_one({"user_id": int(user_id)})
    return doc.get("action") if doc else None


@app.on_message(filters.private & filters.photo)
async def handle_photo(client, message):
    pending = await get_pending_action(message.from_user.id)
    if pending == "await_thumb":
        # save the photo file_id
        photo = message.photo
        file_id = photo.file_id
        await set_user_config(message.from_user.id, {"thumbnail_file_id": file_id})
        await set_pending_action(message.from_user.id, None)
        await message.reply_text("Thumbnail saved as your permanent thumbnail.")
    else:
        # ignore other photos
        pass


# Media handler (video or document)
@app.on_message(filters.private & (filters.video | filters.document))
async def handle_media(client, message):
    # verify file_size
    doc = message.document or message.video
    size = getattr(doc, "file_size", 0) or 0
    bot_conf = await get_bot_config()
    max_size = bot_conf.get("max_file_size", MAX_FILE_SIZE)
    if size and size > max_size:
        await message.reply_text(f"File too large. Max allowed: {max_size} bytes")
        return

    # create task id and record
    task_id = str(uuid.uuid4())
    task = {
        "_id": task_id,
        "user_id": message.from_user.id,
        "status": "options",
        "file_size": size,
        "file_id": doc.file_id,
        "created_at": asyncio.get_event_loop().time()
    }
    await create_task_record(task)
    msg = await message.reply_text("Received file. Preparing inline options...")
    # update chat message pointers
    await update_task_record(task_id, {"chat_id": msg.chat.id, "msg_id": msg.message_id})

    # show inline options: quality buttons + start + audio/hardsub selection buttons
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("360p", callback_data=f"pickq:{task_id}:360"), InlineKeyboardButton("480p", callback_data=f"pickq:{task_id}:480")],
        [InlineKeyboardButton("720p", callback_data=f"pickq:{task_id}:720"), InlineKeyboardButton("1080p", callback_data=f"pickq:{task_id}:1080")],
        [InlineKeyboardButton("Toggle Hardsub (user default)", callback_data=f"pick_hardsub:{task_id}")],
        [InlineKeyboardButton("Select audio (keep ENG)", callback_data=f"pick_audio:{task_id}:eng"), InlineKeyboardButton("Select audio (keep ALL)", callback_data=f"pick_audio:{task_id}:all")],
        [InlineKeyboardButton("Start Processing ▶", callback_data=f"start:{task_id}")],
    ])
    await msg.edit_text("Choose quality / options and press Start. All options are inline buttons.", reply_markup=kb)


# Inline callback router (handles both user and admin callbacks)
@app.on_callback_query()
async def on_cb(client, cq):
    data = cq.data or ""
    # pick quality
    if data.startswith("pickq:"):
        _, task_id, q = data.split(":", 2)
        await update_task_record(task_id, {"selected_quality": int(q)})
        await cq.answer(f"Quality set to {q}p")
        return
    if data.startswith("pick_hardsub:"):
        _, task_id = data.split(":", 1)
        cur = await db.tasks.find_one({"_id": task_id})
        if not cur:
            await cq.answer("Task not found", show_alert=True)
            return
        new_state = not cur.get("hardsub_user_choice", False)
        await update_task_record(task_id, {"hardsub_user_choice": new_state})
        await cq.answer(f"Hardsub preference toggled to {'ON' if new_state else 'OFF'}")
        return
    if data.startswith("pick_audio:"):
        # two options: keep eng or keep all
        _, task_id, choice = data.split(":", 2)
        if choice == "all":
            await update_task_record(task_id, {"audio_keep": []})
            await cq.answer("Will keep all audio tracks")
        else:
            await update_task_record(task_id, {"audio_keep": [choice]})
            await cq.answer(f"Will keep audio: {choice}")
        return
    if data.startswith("start:"):
        _, task_id = data.split(":", 1)
        task = await db.tasks.find_one({"_id": task_id})
        if not task:
            await cq.answer("Task not found", show_alert=True)
            return
        await cq.answer("Queued — processing will start shortly.")
        # edit message to show queued
        try:
            await cq.message.edit_text("Queued — starting processing now. Progress will update here.")
        except Exception:
            pass
        # start processing in background
        asyncio.create_task(process_task(task_id))
        return

    # user settings inline actions
    if data == "set_quality":
        # toggle user default quality through options list
        uid = cq.from_user.id
        conf = await get_user_config(uid)
        options = [360, 480, 720, 1080]
        cur = conf.get("default_quality", 720)
        try:
            i = options.index(cur)
            nxt = options[(i + 1) % len(options)]
        except ValueError:
            nxt = 720
        await set_user_config(uid, {"default_quality": nxt})
        await cq.answer(f"Default quality set to {nxt}p")
        await cq.message.edit_reply_markup(reply_markup=settings_keyboard(await get_user_config(uid)))
        return
    if data == "toggle_hardsub":
        uid = cq.from_user.id
        conf = await get_user_config(uid)
        n = not conf.get("hardsub", False)
        await set_user_config(uid, {"hardsub": n})
        await cq.answer("Hardsub toggled")
        await cq.message.edit_reply_markup(reply_markup=settings_keyboard(await get_user_config(uid)))
        return
    if data == "reset_settings":
        uid = cq.from_user.id
        await set_user_config(uid, {"default_quality": 720, "hardsub": False, "audio_keep": [], "thumbnail_file_id": None})
        await cq.answer("Settings reset")
        await cq.message.edit_reply_markup(reply_markup=settings_keyboard(await get_user_config(uid)))
        return
    if data == "save_thumb":
        # set pending action for user and instruct them to send a photo
        await set_pending_action(cq.from_user.id, "await_thumb")
        await cq.answer("Send a photo and it will be saved as your permanent thumbnail.")
        await cq.message.edit_text("Now send a photo (it will be saved as your default thumbnail).")
        return
    if data == "set_audio":
        # quick helper: in settings, toggling to 'keep ENG' as an example
        uid = cq.from_user.id
        await set_user_config(uid, {"audio_keep": ["eng"]})
        await cq.answer("Audio keep set to ENG")
        await cq.message.edit_reply_markup(reply_markup=settings_keyboard(await get_user_config(uid)))
        return

    # Admin callbacks: prefixed with adm_
    if data.startswith("adm_"):
        # only allow admins
        if cq.from_user.id not in ADMINS:
            await cq.answer("Not authorized", show_alert=True)
            return
        if data == "adm_stats":
            s = await get_stats()
            await cq.message.edit_text(f"Users: {s['users']}\nTasks total: {s['tasks_total']}\nActive: {s['active_tasks']}")
            await cq.answer("Stats shown")
            return
        if data == "adm_max_tasks":
            await cq.message.edit_text("Send a message with the new integer value for max concurrent tasks.")
            await cq.answer("Send new value as a message")
            # admin text handler will process next message
            return
        if data == "adm_max_size":
            await cq.message.edit_text("Send a message with the new max file size in bytes.")
            await cq.answer("Send new value as a message")
            return
        if data == "adm_tokens":
            await cq.message.edit_text("Reply with a token to add to API tokens.")
            await cq.answer("Send token string")
            return

    # fallback
    await cq.answer()


# Admin text handler: used to set numeric fields or add token
@app.on_message(filters.private & filters.user(*ADMINS) & filters.text)
async def admin_text(client, message):
    text = message.text.strip()
    # try to interpret as integer
    try:
        n = int(text)
        # naive: set max_concurrent_tasks
        # In production you'd want a more robust state machine to know what the admin intended.
        await set_bot_config({"max_concurrent_tasks": n})
        await message.reply_text(f"Updated bot max_concurrent_tasks to {n}")
        return
    except ValueError:
        # treat as token
        await add_api_token(text)
        await message.reply_text("API token added.")
        return


# -----------------------------
# Task processing function
# -----------------------------
# Semaphore to limit concurrency
task_semaphore = asyncio.Semaphore(MAX_CONCURRENT_TASKS)


async def process_task(task_id: str):
    tlog = get_task_logger(task_id)
    tlog.info("Processing task %s", task_id)
    task = await db.tasks.find_one({"_id": task_id})
    if not task:
        tlog.error("Task not found in DB")
        return
    await update_task_record(task_id, {"status": "running"})
    # load user config
    uid = task.get("user_id")
    user_conf = await get_user_config(uid)
    selected_quality = task.get("selected_quality") or user_conf.get("default_quality", 720)
    hardsub_user_choice = task.get("hardsub_user_choice", None)
    if hardsub_user_choice is None:
        hardsub = user_conf.get("hardsub", False)
    else:
        hardsub = hardsub_user_choice
    audio_keep = task.get("audio_keep")
    if audio_keep is None:
        audio_keep = user_conf.get("audio_keep", [])

    chat_id = task.get("chat_id")
    msg_id = task.get("msg_id")
    file_id = task.get("file_id")
    # prepare paths
    downloads_dir = os.path.join(os.path.dirname(__file__), "downloads")
    os.makedirs(downloads_dir, exist_ok=True)
    input_path = os.path.join(downloads_dir, f"{task_id}_in")
    output_path = os.path.join(downloads_dir, f"{task_id}_out.mp4")
    # download
    try:
        await app.download_media(file_id, file_name=input_path)
    except Exception as e:
        tlog.exception("Download failed: %s", e)
        await app.send_message(chat_id, f"Download failed: {e}")
        await update_task_record(task_id, {"status": "failed", "error": str(e)})
        return

    # optional: check for sidecar subtitle in task - not implemented auto
    subtitle_path = None

    # progress callback that edits the original message
    async def on_progress(percent: float, meta: dict):
        try:
            percent_text = f"Processing: {percent:.1f}%\n"
            # add a simple bar
            bars = 12
            filled = int(percent / 100.0 * bars)
            bar_text = "[" + "█" * filled + "-" * (bars - filled) + "]"
            text = percent_text + bar_text + f"  {percent:.1f}%"
            try:
                await app.edit_message_text(chat_id, msg_id, text)
            except Exception:
                # sometimes editing old message fails; ignore
                pass
            # update DB with last_percent occasionally
            if percent % 5 < 0.5:
                await update_task_record(task_id, {"last_percent": percent})
        except Exception:
            pass

    # run the actual ffmpeg with concurrency limit
    try:
        async with task_semaphore:
            rc = await run_ffmpeg_task(task_id, input_path, output_path, int(selected_quality), bool(hardsub), subtitle_path, audio_keep, on_progress)
    except Exception as e:
        tlog.exception("Processing error: %s", e)
        await app.send_message(chat_id, f"Processing error: {e}")
        await update_task_record(task_id, {"status": "failed", "error": str(e)})
        # cleanup files
        try:
            os.remove(input_path)
        except Exception:
            pass
        return

    if rc != 0:
        tlog.error("ffmpeg returned code %s", rc)
        await app.send_message(chat_id, "Processing failed. Check logs.")
        await update_task_record(task_id, {"status": "failed", "exit_code": rc})
        try:
            os.remove(input_path)
            if os.path.exists(output_path):
                os.remove(output_path)
        except Exception:
            pass
        return

    # upload the result
    try:
        sent = await app.send_document(chat_id, output_path)
        file_url = f"https://t.me/{(await app.get_me()).username}/{sent.message_id}"
        short = await shorten_url(file_url)
        await app.send_message(chat_id, f"Done! Download: {short}")
        await update_task_record(task_id, {"status": "done", "file_message_id": sent.message_id})
    except Exception as e:
        tlog.exception("Upload failed: %s", e)
        await app.send_message(chat_id, f"Upload failed: {e}")
        await update_task_record(task_id, {"status": "failed", "error": str(e)})
    finally:
        # cleanup
        try:
            os.remove(input_path)
        except Exception:
            pass
        try:
            os.remove(output_path)
        except Exception:
            pass
        tlog.info("Task finished and cleaned up.")


# -----------------------------
# FastAPI (small token-protected API)
# -----------------------------
api_app = FastAPI(title="VideoBot Single API")


async def require_api_token(x_api_key: Optional[str] = Header(None)):
    if not x_api_key:
        raise HTTPException(status_code=401, detail="Missing X-API-KEY")
    ok = await is_valid_api_token(x_api_key)
    if not ok:
        raise HTTPException(status_code=403, detail="Invalid API token")
    return True


@api_app.get("/health")
async def health():
    return {"ok": True}


@api_app.get("/status")
async def status(x_api_key: Optional[str] = Header(None)):
    await require_api_token(x_api_key)
    return {"ok": True, "stats": await get_stats()}


# -----------------------------
# Start both Pyrogram and FastAPI together
# -----------------------------
async def main():
    # Ensure bot_config exists
    await get_bot_config()
    # run pyrogram client and uvicorn (FastAPI) concurrently
    config_uvicorn = uvicorn.Config(api_app, host="0.0.0.0", port=PORT, loop="asyncio", log_level="info")
    server = uvicorn.Server(config_uvicorn)

    # Start Pyrogram client and uvicorn server concurrently
    async with app:
        # run uvicorn in background
        api_task = asyncio.create_task(server.serve())
        logger.info("Bot started. API server running on port %s", PORT)
        # keep the context alive until uvicorn stops (or cancelled)
        await api_task


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Shutting down...")
