# bot.py
import asyncio
import os
import json
import time
import signal
from pathlib import Path
import discord
from discord.ext import commands
import yt_dlp
import random
import re
from difflib import get_close_matches
from typing import Optional
from humor_engine import get_humor_for_message

# ---------------- KONFIGURACJA ----------------
TOKEN_FILE = Path(r"E:\apka\bot\token.txt")
FFMPEG_EXE = Path(r"E:\apka\bot\ffmpeg-8.0.1-essentials_build\bin\ffmpeg.exe")
BOT_PREFIX = "!"
AUTO_JOIN_ON_GUILD_JOIN = False
AUTO_SELECT_TIMEOUT = 7  # sekundy do automatycznego wyboru, jeśli brak odpowiedzi

# CACHE: plik cache obok skryptu
CACHE_FILE = Path(__file__).resolve().parent / "cache_search.json"
CACHE_TTL = 60 * 60 * 6  # 6 godzin TTL dla wyników wyszukiwania

# RATE LIMIT / DEBOUNCE
ENABLE_USER_COOLDOWN = False
USER_SEARCH_COOLDOWN = 1.0
# ----------------------------------------------

def load_token(path: Path) -> str:
    env_token = os.environ.get("DISCORD_TOKEN")
    if env_token:
        return env_token.strip()
    if not path.exists():
        raise FileNotFoundError(f"Nie znaleziono pliku tokenu: {path}")
    token = path.read_text(encoding="utf-8").strip()
    if not token:
        raise ValueError("Plik token.txt jest pusty.")
    return token

intents = discord.Intents.default()
intents.message_content = True
intents.voice_states = True
intents.guilds = True
intents.members = True

bot = commands.Bot(command_prefix=BOT_PREFIX, intents=intents)

queues: dict[int, asyncio.Queue] = {}
play_locks: dict[int, asyncio.Lock] = {}

pending_searches = {'user': {}, 'channel': {}}
user_last_search: dict[int, float] = {}

search_cache: dict[str, dict] = {}
cache_lock = asyncio.Lock()

# Autoplay state per guild and last played title
autoplay_enabled: dict[int, bool] = {}
last_played_title: dict[int, str] = {}

YTDL_OPTS = {
    'format': 'bestaudio/best',
    'quiet': True,
    'no_warnings': True,
    'default_search': 'ytsearch',
    'source_address': '0.0.0.0'
}
FFMPEG_OPTIONS = {
    'before_options': '-reconnect 1 -reconnect_streamed 1 -reconnect_delay_max 5',
    'options': '-vn'
}
ytdl = yt_dlp.YoutubeDL(YTDL_OPTS)

# ---------------- CACHE HELPERS ----------------
def _load_cache_from_disk_sync(path: Path):
    if not path.exists():
        return {}
    try:
        with path.open("r", encoding="utf-8") as f:
            data = json.load(f)
        return data
    except Exception:
        return {}

def _save_cache_to_disk_sync(path: Path, data: dict):
    try:
        tmp = path.with_suffix(".tmp")
        with tmp.open("w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False)
        tmp.replace(path)
    except Exception:
        pass

async def load_cache_from_disk():
    global search_cache
    data = await asyncio.to_thread(_load_cache_from_disk_sync, CACHE_FILE)
    now = time.time()
    cleaned = {}
    for k, v in data.items():
        ts = v.get('ts', 0)
        if now - ts < CACHE_TTL:
            cleaned[k] = v
    async with cache_lock:
        search_cache = cleaned

async def save_cache_to_disk():
    async with cache_lock:
        data = dict(search_cache)
    await asyncio.to_thread(_save_cache_to_disk_sync, CACHE_FILE, data)

async def get_cached_search(query: str):
    q = query.strip().lower()
    async with cache_lock:
        entry = search_cache.get(q)
        if not entry:
            return None
        if time.time() - entry.get('ts', 0) > CACHE_TTL:
            search_cache.pop(q, None)
            return None
        return entry.get('results')

async def set_cached_search(query: str, results):
    q = query.strip().lower()
    entry = {'ts': time.time(), 'results': results}
    async with cache_lock:
        search_cache[q] = entry
    asyncio.create_task(save_cache_to_disk())

# ---------------- END CACHE HELPERS ----------------

def ensure_queue(guild_id: int) -> asyncio.Queue:
    if guild_id not in queues:
        queues[guild_id] = asyncio.Queue()
    return queues[guild_id]

def ensure_lock(guild_id: int) -> asyncio.Lock:
    if guild_id not in play_locks:
        play_locks[guild_id] = asyncio.Lock()
    return play_locks[guild_id]

def bot_has_voice_perms(channel: discord.VoiceChannel) -> bool:
    perms = channel.permissions_for(channel.guild.me)
    return perms.connect and perms.speak

async def ensure_connected_to_author_voice(ctx_or_message) -> tuple:
    author = getattr(ctx_or_message, "author", None) or ctx_or_message.author
    guild = getattr(ctx_or_message, "guild", None) or (author.guild if hasattr(author, "guild") else None)
    if author is None or guild is None:
        return None, "Brak kontekstu serwera."
    if not getattr(author, "voice", None) or not author.voice.channel:
        return None, "Musisz być na kanale głosowym, aby odtworzyć muzykę."
    target_channel = author.voice.channel
    try:
        vc = guild.voice_client
        if vc is None:
            await target_channel.connect()
            return target_channel, None
        else:
            if vc.channel.id != target_channel.id:
                await vc.move_to(target_channel)
            return target_channel, None
    except Exception as e:
        return None, f"Nie mogę połączyć się z kanałem głosowym: {e}"

async def extract_info_async(query: str, loop: Optional[asyncio.AbstractEventLoop] = None):
    loop = loop or asyncio.get_event_loop()
    return await loop.run_in_executor(None, lambda: ytdl.extract_info(query, download=False))

async def search_youtube(query: str, max_results: int = 5):
    qkey = query.strip().lower()
    cached = await get_cached_search(qkey)
    if cached is not None:
        return cached[:max_results]
    try:
        info = await extract_info_async(f"ytsearch{max_results}:{query}")
    except Exception:
        return []
    results = []
    if not info:
        return results
    entries = info.get('entries', [])
    for e in entries:
        url = e.get('url') or e.get('webpage_url')
        title = e.get('title', 'Unknown')
        if url:
            results.append({'title': title, 'url': url})
    await set_cached_search(qkey, results[:10])
    return results

# --- Playback ---
async def _play_next_from_queue(guild_id: int, ctx_channel: discord.abc.Messageable, ffmpeg_exe: str):
    """
    Pobiera następny element z kolejki i odtwarza.
    Po zakończeniu utworu uruchamia _after_track_finished, który może uruchomić autoplay.
    """
    q = queues.get(guild_id)
    if not q:
        return
    try:
        item = await q.get()
    except asyncio.CancelledError:
        return

    guild = ctx_channel.guild
    voice_client = guild.voice_client
    if voice_client is None:
        await ctx_channel.send("Nie jestem połączony z kanałem głosowym.")
        return

    source_url = item['url']
    title = item.get('title', 'Unknown')

    # zapamiętaj ostatnio odtwarzany tytuł
    last_played_title[guild_id] = title

    try:
        player = discord.FFmpegPCMAudio(source_url, executable=ffmpeg_exe, **FFMPEG_OPTIONS)

        def after_play(error):
            # after_play działa w wątku odtwarzacza, więc przekazujemy zadanie do pętli
            if error:
                print(f"Error during playback: {error}")
            # po zakończeniu utworu sprawdź autoplay i uruchom kolejny element
            asyncio.run_coroutine_threadsafe(_after_track_finished(guild_id, ctx_channel, ffmpeg_exe), bot.loop)

        voice_client.play(player, after=after_play)
        await ctx_channel.send(f"Teraz gra: **{title}**")
    except Exception as e:
        await ctx_channel.send(f"Błąd podczas odtwarzania: {e}")
        asyncio.create_task(_play_next_from_queue(guild_id, ctx_channel, ffmpeg_exe))

async def _after_track_finished(guild_id: int, ctx_channel: discord.abc.Messageable, ffmpeg_exe: str):
    """
    Wywoływane po zakończeniu utworu. Jeśli w kolejce są kolejne utwory, _play_next_from_queue je odtworzy.
    Jeśli kolejka jest pusta i autoplay jest włączony, generujemy nową listę podobnych utworów i dodajemy do kolejki.
    """
    # krótka pauza, żeby kolejka zdążyła się zaktualizować
    await asyncio.sleep(0.2)

    q = queues.get(guild_id)
    if q is None:
        return

    # jeśli są elementy w kolejce, po prostu odtwórz następny
    if not q.empty():
        asyncio.create_task(_play_next_from_queue(guild_id, ctx_channel, ffmpeg_exe))
        return

    # jeśli autoplay wyłączony — nic nie rób
    if not autoplay_enabled.get(guild_id, False):
        return

    # spróbuj wygenerować podobne utwory na podstawie ostatnio odtwarzanego tytułu
    last_title = last_played_title.get(guild_id)
    if not last_title:
        return

    # prosty sposób na "podobne": użyj tytułu + "similar songs" lub "related"
    # można to później ulepszyć
    query_variants = [
        f"{last_title} similar songs",
        f"{last_title} related songs",
        f"similar to {last_title}",
        f"{last_title} playlist"
    ]
    candidates = []
    for qv in query_variants:
        try:
            candidates = await search_youtube(qv, max_results=5)
        except Exception:
            candidates = []
        if candidates:
            break

    if not candidates:
        # jeśli nie znaleziono niczego podobnego, spróbuj ogólnego wyszukiwania po tytule
        try:
            candidates = await search_youtube(last_title, max_results=5)
        except Exception:
            candidates = []

    if not candidates:
        # nic nie znaleziono — kończ
        try:
            await ctx_channel.send("Autoplay: nie udało się znaleźć podobnych utworów.")
        except Exception:
            pass
        return

    # utwórz pending dla nowej listy, ale nie pokazuj jej automatycznie (shown=False)
    entry = {
        'user_id': None,
        'guild_id': ctx_channel.guild.id,
        'channel_id': ctx_channel.id,
        'candidates': candidates,
        'key_type': 'channel',
        'last_index': -1,
        'locked': False,
        'shown': False
    }
    # zapisz pending (nadpisz poprzednie)
    pending_searches['channel'][ctx_channel.id] = entry
    # nie przypisujemy do 'user' bo to autoplay

    # dodaj wszystkie utwory do kolejki
    q = ensure_queue(guild_id)
    for c in candidates:
        await q.put({'url': c['url'], 'title': c['title']})

    # rozpocznij odtwarzanie jeśli idle
    await schedule_play_if_idle(guild_id, ctx_channel)

async def schedule_play_if_idle(guild_id: int, ctx_channel: discord.abc.Messageable):
    lock = ensure_lock(guild_id)
    async with lock:
        guild = ctx_channel.guild
        vc = guild.voice_client
        if vc and (not vc.is_playing() and not vc.is_paused()):
            ffmpeg_exe = str(FFMPEG_EXE) if FFMPEG_EXE.exists() else "ffmpeg"
            asyncio.create_task(_play_next_from_queue(guild_id, ctx_channel, ffmpeg_exe))

async def play_from_url(guild_id: int, ctx_channel: discord.abc.Messageable, url: str, title: str, ctx_or_message=None):
    if ctx_or_message:
        voice_channel, err = await ensure_connected_to_author_voice(ctx_or_message)
        if err:
            try:
                await ctx_channel.send(err)
            except Exception:
                pass
            return
    q = ensure_queue(guild_id)
    await q.put({'url': url, 'title': title})
    await schedule_play_if_idle(guild_id, ctx_channel)

async def play_immediately(guild_id: int, ctx_channel: discord.abc.Messageable, url: str, title: str, ctx_or_message=None):
    if ctx_or_message:
        voice_channel, err = await ensure_connected_to_author_voice(ctx_or_message)
        if err:
            try:
                await ctx_channel.send(err)
            except Exception:
                pass
            return

    lock = ensure_lock(guild_id)
    async with lock:
        q = ensure_queue(guild_id)
        items = []
        try:
            while True:
                items.append(q.get_nowait())
        except asyncio.QueueEmpty:
            pass

        new_item = {'url': url, 'title': title}
        new_q = asyncio.Queue()
        await new_q.put(new_item)
        for it in items:
            await new_q.put(it)

        queues[guild_id] = new_q

        guild = ctx_channel.guild
        vc = guild.voice_client
        ffmpeg_exe = str(FFMPEG_EXE) if FFMPEG_EXE.exists() else "ffmpeg"

        if vc and vc.is_playing():
            try:
                vc.stop()
            except Exception:
                asyncio.create_task(_play_next_from_queue(guild_id, ctx_channel, ffmpeg_exe))
        else:
            asyncio.create_task(_play_next_from_queue(guild_id, ctx_channel, ffmpeg_exe))

# --- AUTO-SELECT: JEDNORAZOWY, BEZ NIESKOŃCZONEJ ROTACJI ---
async def _auto_select_after_timeout(entry: dict, timeout: int):
    await asyncio.sleep(timeout)

    key_type = entry['key_type']
    key = entry['user_id'] if key_type == 'user' else entry['channel_id']
    pending = pending_searches[key_type].get(key)

    # Jeśli pending zniknął lub jest innym obiektem → nic nie rób
    if not pending or pending is not entry:
        return

    # Jeśli użytkownik coś wybrał (locked) → nie rób auto-select
    if pending.get('locked'):
        pending.pop('task', None)
        return

    candidates = pending.get('candidates', [])
    if not candidates:
        pending.pop('task', None)
        return

    # Auto-wybór: zawsze pierwszy element z listy
    chosen = candidates[0]
    pending['last_index'] = 0  # aktualnie gra pierwszy

    channel = bot.get_channel(pending['channel_id'])
    if channel:
        try:
            await channel.send(f"Brak odpowiedzi — wybieram automatycznie po {timeout} s: **{chosen['title']}**")
        except Exception:
            pass

        # spróbuj połączyć się z kanałem autora
        user_id = pending.get('user_id')
        guild = bot.get_guild(pending['guild_id'])
        member = guild.get_member(user_id) if guild else None

        if member and getattr(member, "voice", None) and member.voice.channel:
            class _Tmp:
                def __init__(self, author, guild):
                    self.author = author
                    self.guild = guild
            tmp = _Tmp(member, guild)
            voice_channel, err = await ensure_connected_to_author_voice(tmp)
            if err:
                try:
                    await channel.send(f"Nie mogę połączyć się z kanałem głosowym przed automatycznym wyborem: {err}")
                except Exception:
                    pass
                return

        try:
            await play_immediately(pending['guild_id'], channel, chosen['url'], chosen['title'], ctx_or_message=None)
        except Exception as e:
            try:
                await channel.send(f"Nie udało się automatycznie odtworzyć utworu: {e}")
            except Exception:
                pass

    # KLUCZOWE: po tym jednym auto-wyborze NIE MA już dalszej auto-rotacji
    pending['locked'] = True      # traktuj jakby użytkownik dokonał wyboru
    pending.pop('task', None)     # usuń task, żeby nie odpalał się ponownie
    pending['shown'] = False      # lista nadal nie była pokazana (jeśli nie była)

# --- Komendy ---
@bot.command(name='join')
async def join(ctx: commands.Context):
    if not ctx.author.voice or not ctx.author.voice.channel:
        return await ctx.send("Musisz być na kanale głosowym, aby mnie przywołać.")
    channel = ctx.author.voice.channel
    if not bot_has_voice_perms(channel):
        return await ctx.send("Nie mam uprawnień Connect/Speak w tym kanale.")
    try:
        if ctx.voice_client is None:
            await channel.connect()
            await ctx.send(f"Dołączyłem do kanału: **{channel.name}**")
        else:
            if ctx.voice_client.channel.id != channel.id:
                await ctx.voice_client.move_to(channel)
                await ctx.send(f"Przeniosłem się do kanału: **{channel.name}**")
            else:
                await ctx.send("Już jestem na Twoim kanale głosowym.")
    except Exception as e:
        await ctx.send(f"Nie mogę dołączyć: {e}")

@bot.command(name='leave')
async def leave(ctx: commands.Context):
    vc = ctx.voice_client
    if not vc:
        return await ctx.send("Nie jestem połączony.")
    try:
        queues.pop(ctx.guild.id, None)
        await vc.disconnect()
        await ctx.send("Rozłączyłem się i wyczyściłem kolejkę.")
    except Exception as e:
        await ctx.send(f"Błąd przy rozłączaniu: {e}")

@bot.command(name='play')
async def play_cmd(ctx: commands.Context, *, query: str):
    if ENABLE_USER_COOLDOWN:
        now = time.time()
        last = user_last_search.get(ctx.author.id, 0)
        if now - last < USER_SEARCH_COOLDOWN:
            return await ctx.send("Proszę chwilę poczekać przed kolejnym wyszukiwaniem.")
        user_last_search[ctx.author.id] = now
    asyncio.create_task(handle_play_request(ctx, query))

@bot.command(name='skip')
async def skip(ctx: commands.Context):
    vc = ctx.voice_client
    if not vc or not vc.is_playing():
        return await ctx.send("Nic nie gra.")
    vc.stop()
    await ctx.send("Pominięto utwór.")

@bot.command(name='pause')
async def pause(ctx: commands.Context):
    vc = ctx.voice_client
    if not vc or not vc.is_playing():
        return await ctx.send("Nic nie gra.")
    vc.pause()
    await ctx.send("Pauza.")

@bot.command(name='resume')
async def resume(ctx: commands.Context):
    vc = ctx.voice_client
    if not vc or not vc.is_paused():
        return await ctx.send("Brak pauzy.")
    vc.resume()
    await ctx.send("Wznawiam odtwarzanie.")

@bot.command(name='nowplaying')
async def nowplaying(ctx: commands.Context):
    q = queues.get(ctx.guild.id)
    if not q or q.empty():
        return await ctx.send("Brak utworów w kolejce.")
    try:
        item = q._queue[0]
        await ctx.send(f"Aktualnie w kolejce: **{item.get('title','Unknown')}**")
    except Exception:
        await ctx.send("Nie mogę odczytać kolejki.")

@bot.command(name='stop')
async def stop(ctx: commands.Context):
    vc = ctx.voice_client
    if not vc:
        return await ctx.send("Nie jestem połączony.")
    queues.pop(ctx.guild.id, None)
    try:
        await vc.disconnect()
        await ctx.send("Zatrzymano odtwarzanie i rozłączyłem się.")
    except Exception as e:
        await ctx.send(f"Błąd przy zatrzymaniu: {e}")

@bot.command(name='autoplay')
async def cmd_autoplay(ctx: commands.Context, *, mode: Optional[str] = None):
    """
    Komenda: !autoplay on/off
    Jeśli wywołana bez argumentu, przełącza stan.
    """
    gid = ctx.guild.id
    current = autoplay_enabled.get(gid, False)
    if mode:
        m = mode.strip().lower()
        if m in ('on', 'włącz', 'wlacz', 'start', 'tak'):
            autoplay_enabled[gid] = True
            await ctx.send("Autoplay włączony.")
            return
        if m in ('off', 'wyłącz', 'wylacz', 'stop', 'nie'):
            autoplay_enabled[gid] = False
            await ctx.send("Autoplay wyłączony.")
            return
    # toggle
    autoplay_enabled[gid] = not current
    await ctx.send(f"Autoplay {'włączony' if autoplay_enabled[gid] else 'wyłączony'}.")

@bot.command(name='stopautoplay')
async def cmd_stop_autoplay(ctx: commands.Context):
    gid = ctx.guild.id
    autoplay_enabled[gid] = False
    await ctx.send("Autoplay wyłączony.")

# --- NLP helpers ---
PLAY_KEYWORDS = ['włącz', 'wlacz', 'puść', 'pusc', 'odtwórz', 'odtworz', 'graj', 'daj włącz', 'daj wlacz', 'daj puść', 'wlacz', 'włącz']
NEXT_KEYWORDS = ['następny', 'nastepny', 'dalej', 'next', 'następny utwór', 'następny track', 'następny song', 'następne', 'nastepne']
PREV_KEYWORDS = ['poprzedni', 'poprzednia', 'wstecz', 'cofnij', 'poprzednie', 'poprzednia piosenka']
STOP_KEYWORDS = ['stop', 'zatrzymaj', 'wyłącz', 'wylacz']
PAUSE_KEYWORDS = ['pauza', 'zatrzymaj chwilowo', 'przerwij']
RESUME_KEYWORDS = ['wznów', 'wznow', 'kontynuuj', 'resume']
CHRISTMAS_KEYWORDS = ['świątecz', 'swiatecz', 'bożonarodzeni', 'bozonarodzeni', 'christmas', 'xmas']
RAP_KEYWORDS = ['rap', 'hip hop', 'hip-hop']
GENRE_KEYWORDS = {
    'rap': RAP_KEYWORDS,
    'christmas': CHRISTMAS_KEYWORDS,
    'pop': ['pop'],
    'rock': ['rock'],
    'electronic': ['electro', 'electronic', 'edm'],
    'jazz': ['jazz']
}
AUTOPLAY_ON_KEYWORDS = ['autoplay', 'autoodtwarzanie', 'autoodtwarzaj', 'autoplay on', 'włącz autoplay', 'wlacz autoplay', 'graj podobne', 'kontynuuj automatycznie', 'autoplay on']
AUTOPLAY_OFF_KEYWORDS = ['stop autoplay', 'wyłącz autoplay', 'wylacz autoplay', 'stop autoodtwarzanie', 'zatrzymaj autoodtwarzanie', 'autoplay off']

def normalize_text(s: str) -> str:
    s = s.lower()
    s = s.replace('ó', 'o').replace('ą','a').replace('ę','e').replace('ś','s').replace('ć','c').replace('ż','z').replace('ź','z').replace('ł','l')
    s = re.sub(r'[^\w\s]', ' ', s)
    s = re.sub(r'\s+', ' ', s).strip()
    return s

def detect_intent_and_query(message: str):
    text = normalize_text(message)
    # autoplay on/off detection (prioritize)
    for kw in AUTOPLAY_ON_KEYWORDS:
        if kw in text:
            return ('autoplay_on', None)
    for kw in AUTOPLAY_OFF_KEYWORDS:
        if kw in text:
            return ('autoplay_off', None)
    for kw in NEXT_KEYWORDS:
        if kw in text:
            return ('next', None)
    for kw in PREV_KEYWORDS:
        if kw in text:
            return ('prev', None)
    for kw in STOP_KEYWORDS:
        if kw in text:
            return ('stop', None)
    for kw in PAUSE_KEYWORDS:
        if kw in text:
            return ('pause', None)
    for kw in RESUME_KEYWORDS:
        if kw in text:
            return ('resume', None)
    for kw in PLAY_KEYWORDS:
        if kw in text:
            idx = text.rfind(kw)
            after = message[idx + len(kw):].strip()
            return ('play', after if after else None)
    return (None, None)

# --- Funkcja automatycznego wyświetlania listy ---
async def show_pending_list(pending, channel):
    candidates = pending.get('candidates', [])
    last = pending.get('last_index', -1)

    msg = ["Lista utworów:"]
    for i, c in enumerate(candidates, start=1):
        if i - 1 == last:
            msg.append(f"**{i}. {c['title']}**  ← aktualnie wybrany")
        else:
            msg.append(f"{i}. {c['title']}")

    await channel.send("\n".join(msg))
    pending['shown'] = True

# --- Main play handler ---
async def handle_play_request(ctx: commands.Context, query: Optional[str]):
    if not ctx.author.voice or not ctx.author.voice.channel:
        return await ctx.send("Musisz być na kanale głosowym, aby odtworzyć muzykę.")
    voice_channel = ctx.author.voice.channel
    if not bot_has_voice_perms(voice_channel):
        return await ctx.send("Nie mam uprawnień Connect/Speak w tym kanale.")

    voice_client = ctx.voice_client
    try:
        if voice_client is None:
            await voice_channel.connect()
        else:
            if voice_client.channel.id != voice_channel.id:
                await voice_client.move_to(voice_channel)
    except Exception as e:
        return await ctx.send(f"Nie mogę dołączyć do kanału: {e}")

    if not query:
        random_queries = [
            "popular music 2024", "top hits playlist", "best songs playlist",
            "chill music playlist", "party hits playlist"
        ]
        chosen = random.choice(random_queries)
        candidates = await search_youtube(chosen, max_results=1)
        if not candidates:
            return await ctx.send("Nie udało się znaleźć losowego utworu.")
        c = candidates[0]
        await play_from_url(ctx.guild.id, ctx.channel, c['url'], c['title'], ctx_or_message=ctx)
        return

    if query.startswith("http://") or query.startswith("https://"):
        try:
            info = await extract_info_async(query)
            if 'entries' in info and info['entries']:
                entry = info['entries'][0]
            else:
                entry = info
            url = entry.get('url') or entry.get('webpage_url')
            title = entry.get('title', 'Unknown')
            if not url:
                return await ctx.send("Nie udało się uzyskać URL z podanego linku.")
            await play_from_url(ctx.guild.id, ctx.channel, url, title, ctx_or_message=ctx)
            return
        except Exception as e:
            return await ctx.send(f"Błąd przy przetwarzaniu linku: {e}")

    norm = normalize_text(query)
    for genre, keys in GENRE_KEYWORDS.items():
        for k in keys:
            if k in norm:
                if genre == 'christmas':
                    q = "christmas music playlist"
                else:
                    q = f"{genre} music playlist"
                candidates = await search_youtube(q, max_results=3)
                if not candidates:
                    return await ctx.send(f"Nie znalazłem playlisty dla: {genre}")
                c = candidates[0]
                await play_from_url(ctx.guild.id, ctx.channel, c['url'], c['title'], ctx_or_message=ctx)
                return

    candidates = await search_youtube(query, max_results=5)
    if not candidates:
        return await ctx.send("Nie znalazłem nic pasującego do tego zapytania.")
    titles = [c['title'] for c in candidates]
    close = get_close_matches(query, titles, n=1, cutoff=0.7)
    if len(candidates) == 1 or (close and close[0].lower() == titles[0].lower()):
        c = candidates[0]
        # jeśli jednoznaczny wynik — odtwarzamy, ale nie pokazujemy listy
        await play_from_url(ctx.guild.id, ctx.channel, c['url'], c['title'], ctx_or_message=ctx)
        # jeśli chcesz zachować pending mimo jednoznacznego wyboru, ustaw shown=False
        entry = {
            'user_id': ctx.author.id,
            'guild_id': ctx.guild.id,
            'channel_id': ctx.channel.id,
            'candidates': candidates,
            'key_type': 'channel',
            'last_index': 0,
            'locked': False,
            'shown': False
        }
        pending_searches['user'][ctx.author.id] = entry
        pending_searches['channel'][ctx.channel.id] = entry
        return

    # pokazujemy listę i tworzymy pending (lista pokazana)
    msg_lines = ["Znalazłem kilka dopasowań. Wybierz numer (1-5) lub wpisz pełny tytuł:"]
    for i, c in enumerate(candidates, start=1):
        msg_lines.append(f"{i}. {c['title']}")
    await ctx.send("\n".join(msg_lines))

    prev_channel = pending_searches['channel'].get(ctx.channel.id)
    if prev_channel and prev_channel.get('task'):
        try:
            prev_channel['task'].cancel()
        except Exception:
            pass
    prev_user = pending_searches['user'].get(ctx.author.id)
    if prev_user and prev_user.get('task'):
        try:
            prev_user['task'].cancel()
        except Exception:
            pass

    entry = {
        'user_id': ctx.author.id,
        'guild_id': ctx.guild.id,
        'channel_id': ctx.channel.id,
        'candidates': candidates,
        'key_type': 'channel',
        'last_index': -1,
        'locked': False,
        'shown': True  # lista została pokazana
    }
    task = asyncio.create_task(_auto_select_after_timeout(entry, AUTO_SELECT_TIMEOUT))
    entry['task'] = task
    pending_searches['user'][ctx.author.id] = entry
    pending_searches['channel'][ctx.channel.id] = entry

# --- Message handling ---

@bot.event
async def on_message(message: discord.Message):
    if message.author.bot:
        return

    humor_response = get_humor_for_message(message.content, message.author.name)
    if humor_response:
        await message.channel.send(humor_response)
        return

    content = message.content.strip()

    # Quick title paste: "Artysta - Tytuł"
    if ' - ' in content and len(content) > 8 and not content.startswith(BOT_PREFIX):
        async def _try_play_title():
            voice_channel, err = await ensure_connected_to_author_voice(message)
            if err:
                return
            try:
                candidates = await search_youtube(content, max_results=5)
            except Exception:
                candidates = []
            if not candidates:
                simplified = re.sub(r'\(.*?\)', '', content).strip()
                if simplified and simplified != content:
                    candidates = await search_youtube(simplified, max_results=5)
            if not candidates:
                return
            norm_msg = normalize_text(content)
            for idx, c in enumerate(candidates):
                if normalize_text(c['title']) == norm_msg:
                    entry = {
                        'user_id': message.author.id,
                        'guild_id': message.guild.id,
                        'channel_id': message.channel.id,
                        'candidates': candidates,
                        'key_type': 'channel',
                        'last_index': idx,
                        'locked': True,
                        'shown': False
                    }
                    pending_searches['user'][message.author.id] = entry
                    pending_searches['channel'][message.channel.id] = entry
                    await play_immediately(message.guild.id, message.channel, c['url'], c['title'], ctx_or_message=message)
                    return
            titles = [c['title'] for c in candidates]
            matches = get_close_matches(content, titles, n=1, cutoff=0.6)
            if matches:
                chosen = next((c for c in candidates if c['title'] == matches[0]), None)
                if chosen:
                    idx = titles.index(chosen['title'])
                    entry = {
                        'user_id': message.author.id,
                        'guild_id': message.guild.id,
                        'channel_id': message.channel.id,
                        'candidates': candidates,
                        'key_type': 'channel',
                        'last_index': idx,
                        'locked': True,
                        'shown': False
                    }
                    pending_searches['user'][message.author.id] = entry
                    pending_searches['channel'][message.channel.id] = entry
                    await play_immediately(message.guild.id, message.channel, chosen['url'], chosen['title'], ctx_or_message=message)
                    return
            first = candidates[0]
            entry = {
                'user_id': message.author.id,
                'guild_id': message.guild.id,
                'channel_id': message.channel.id,
                'candidates': candidates,
                'key_type': 'channel',
                'last_index': 0,
                'locked': True,
                'shown': False
            }
            pending_searches['user'][message.author.id] = entry
            pending_searches['channel'][message.channel.id] = entry
            await play_immediately(message.guild.id, message.channel, first['url'], first['title'], ctx_or_message=message)

        asyncio.create_task(_try_play_title())
        return

    intent, query = detect_intent_and_query(message.content)

    # handle autoplay NLP intents
    if intent == 'autoplay_on':
        gid = message.guild.id if message.guild else None
        if gid:
            autoplay_enabled[gid] = True
            await message.channel.send("Autoplay włączony.")
        else:
            await message.channel.send("Nie mogę włączyć autoplay — brak kontekstu serwera.")
        return
    if intent == 'autoplay_off':
        gid = message.guild.id if message.guild else None
        if gid:
            autoplay_enabled[gid] = False
            await message.channel.send("Autoplay wyłączony.")
        else:
            await message.channel.send("Nie mogę wyłączyć autoplay — brak kontekstu serwera.")
        return

    # "wlacz 3" / "wlacz nastepne" / "wlacz poprzednie"
    if intent == 'play' and query:
        q_str = query.strip()
        q_norm = normalize_text(q_str)
        for kw in NEXT_KEYWORDS:
            if kw in q_norm:
                intent = 'next'
                query = None
                break
        for kw in PREV_KEYWORDS:
            if kw in q_norm:
                intent = 'prev'
                query = None
                break
        if intent == 'play':
            mnum = re.match(r'^\s*([1-9]\d?)\s*$', q_str)
            if mnum:
                pending_user = pending_searches['user'].get(message.author.id)
                pending_channel = pending_searches['channel'].get(message.channel.id)
                pending = None
                if pending_user and pending_user.get('channel_id') == message.channel.id:
                    pending = pending_user
                elif pending_channel:
                    pending = pending_channel
                if pending:
                    # jeśli lista nie była pokazana → pokaż ją
                    if not pending.get('shown'):
                        channel = bot.get_channel(pending['channel_id'])
                        await show_pending_list(pending, channel)

                    idx = int(mnum.group(1)) - 1
                    candidates = pending.get('candidates', [])
                    if 0 <= idx < len(candidates):
                        c = candidates[idx]
                        pending['last_index'] = idx
                        pending['locked'] = True
                        if pending.get('task'):
                            try:
                                pending['task'].cancel()
                            except Exception:
                                pass
                            pending.pop('task', None)
                        channel = bot.get_channel(pending['channel_id'])
                        asyncio.create_task(play_immediately(pending['guild_id'], channel, c['url'], c['title'], ctx_or_message=message))
                        return
                    else:
                        await message.channel.send("Nieprawidłowy numer. Wybierz numer z listy.")
                        return

    if intent == 'play':
        prev = pending_searches['user'].pop(message.author.id, None)
        if prev and prev.get('task'):
            try:
                prev['task'].cancel()
            except Exception:
                pass
        pending_searches['channel'].pop(message.channel.id, None)
        ctx = await bot.get_context(message)
        asyncio.create_task(handle_play_request(ctx, query))
        return

    pending_user = pending_searches['user'].get(message.author.id)
    pending_channel = pending_searches['channel'].get(message.channel.id)
    pending = None
    if pending_user and pending_user.get('channel_id') == message.channel.id:
        pending = pending_user
    elif pending_channel:
        pending = pending_channel

    # NEXT/PREV: działają na aktualnej liście
    if intent == 'next':
        if pending:
            # jeśli lista nie była pokazana → pokaż ją
            if not pending.get('shown'):
                channel = bot.get_channel(pending['channel_id'])
                await show_pending_list(pending, channel)

            candidates = pending.get('candidates', [])
            last = pending.get('last_index', -1)
            if last < 0:
                last = 0
                pending['last_index'] = 0
            next_idx = last + 1
            if next_idx < len(candidates):
                c = candidates[next_idx]
                pending['last_index'] = next_idx
                pending['locked'] = False
                if pending.get('task'):
                    try:
                        pending['task'].cancel()
                    except Exception:
                        pass
                    pending.pop('task', None)
                # po ręcznym "następne" NIE tworzymy nowego auto-select (czekamy na usera)
                channel = bot.get_channel(pending['channel_id'])
                asyncio.create_task(play_immediately(pending['guild_id'], channel, c['url'], c['title'], ctx_or_message=message))
                return
            else:
                # koniec listy — jeśli autoplay włączony, pozwól autoplay wygenerować nową listę
                gid = message.guild.id if message.guild else None
                if gid and autoplay_enabled.get(gid, False):
                    await message.channel.send("Koniec listy — włączam autoplay, szukam podobnych utworów...")
                    # po prostu zatrzymaj się tutaj; _after_track_finished lub ręczne wywołanie generowania podobnych zajmie się resztą
                    # jednak, żeby natychmiast przejść do generowania, wywołamy _after_track_finished ręcznie
                    ffmpeg_exe = str(FFMPEG_EXE) if FFMPEG_EXE.exists() else "ffmpeg"
                    asyncio.create_task(_after_track_finished(gid, message.channel, ffmpeg_exe))
                    return
                await message.channel.send("To był ostatni element z tej listy.")
                return
        else:
            ctx = await bot.get_context(message)
            asyncio.create_task(skip.callback(ctx))
            return

    if intent == 'prev':
        if pending:
            # jeśli lista nie była pokazana → pokaż ją
            if not pending.get('shown'):
                channel = bot.get_channel(pending['channel_id'])
                await show_pending_list(pending, channel)

            candidates = pending.get('candidates', [])
            last = pending.get('last_index', -1)
            if last <= 0:
                await message.channel.send("Nie ma poprzedniego elementu na tej liście.")
                return
            prev_idx = last - 1
            c = candidates[prev_idx]
            pending['last_index'] = prev_idx
            pending['locked'] = False
            if pending.get('task'):
                try:
                    pending['task'].cancel()
                except Exception:
                    pass
                pending.pop('task', None)
            channel = bot.get_channel(pending['channel_id'])
            asyncio.create_task(play_immediately(pending['guild_id'], channel, c['url'], c['title'], ctx_or_message=message))
            return
        else:
            await message.channel.send("Brak aktywnej listy do cofnięcia.")
            return

    # Pending: wybór numeru / tytułu / fuzzy
    if pending:
        content = message.content.strip()
        candidates = pending.get('candidates', [])

        # jeśli lista nie była pokazana → pokaż ją
        if not pending.get('shown'):
            channel = bot.get_channel(pending['channel_id'])
            await show_pending_list(pending, channel)

        norm_content = normalize_text(content)
        for idx, c in enumerate(candidates):
            if normalize_text(c['title']) == norm_content:
                channel = bot.get_channel(pending['channel_id'])
                pending['last_index'] = idx
                pending['locked'] = True
                if pending.get('task'):
                    try:
                        pending['task'].cancel()
                    except Exception:
                        pass
                    pending.pop('task', None)
                asyncio.create_task(play_immediately(pending['guild_id'], channel, c['url'], c['title'], ctx_or_message=message))
                return

        m = re.search(r'\b(?:nr|numer|n|#)?\s*([1-9]\d?)\b', content, flags=re.IGNORECASE)
        if not m:
            m2 = re.search(r'^\s*([1-9]\d?)\s*$', content)
            if m2:
                m = m2
            else:
                m3 = re.search(r'([1-9]\d?)', content)
                if m3:
                    m = m3
        if m:
            # jeśli lista nie była pokazana → pokaż ją
            if not pending.get('shown'):
                channel = bot.get_channel(pending['channel_id'])
                await show_pending_list(pending, channel)

            idx = int(m.group(1)) - 1
            if 0 <= idx < len(candidates):
                c = candidates[idx]
                channel = bot.get_channel(pending['channel_id'])
                pending['last_index'] = idx
                pending['locked'] = True
                if pending.get('task'):
                    try:
                        pending['task'].cancel()
                    except Exception:
                        pass
                    pending.pop('task', None)
                asyncio.create_task(play_immediately(pending['guild_id'], channel, c['url'], c['title'], ctx_or_message=message))
                return
            else:
                await message.channel.send("Nieprawidłowy numer. Wybierz numer z listy.")
                return

        titles = [c['title'] for c in candidates]
        matches = get_close_matches(content, titles, n=1, cutoff=0.6)
        if matches:
            # jeśli lista nie była pokazana → pokaż ją
            if not pending.get('shown'):
                channel = bot.get_channel(pending['channel_id'])
                await show_pending_list(pending, channel)

            chosen_title = matches[0]
            chosen_idx = titles.index(chosen_title)
            chosen = candidates[chosen_idx]
            channel = bot.get_channel(pending['channel_id'])
            pending['last_index'] = chosen_idx
            pending['locked'] = True
            if pending.get('task'):
                try:
                    pending['task'].cancel()
                except Exception:
                    pass
                pending.pop('task', None)
            asyncio.create_task(play_immediately(pending['guild_id'], channel, chosen['url'], chosen['title'], ctx_or_message=message))
            return

        await message.channel.send("Nie znalazłem dopasowania do tej odpowiedzi. Wpisz numer z listy (np. '3' lub 'numer 3') lub dokładny tytuł.")
        return

    if intent == 'stop':
        ctx = await bot.get_context(message)
        asyncio.create_task(stop.callback(ctx))
        return
    if intent == 'pause':
        ctx = await bot.get_context(message)
        asyncio.create_task(pause.callback(ctx))
        return
    if intent == 'resume':
        ctx = await bot.get_context(message)
        asyncio.create_task(resume.callback(ctx))
        return

    await bot.process_commands(message)

@bot.event
async def on_ready():
    asyncio.create_task(load_cache_from_disk())
    print(f"Zalogowano jako {bot.user} (id: {bot.user.id})")

@bot.event
async def on_guild_join(guild: discord.Guild):
    if not AUTO_JOIN_ON_GUILD_JOIN:
        return
    try:
        for ch in guild.voice_channels:
            if len(ch.members) > 0 and bot_has_voice_perms(ch):
                await ch.connect()
                print(f"[on_guild_join] Dołączono do kanału {ch.name} na serwerze {guild.name}")
                return
        for ch in guild.voice_channels:
            if bot_has_voice_perms(ch):
                await ch.connect()
                print(f"[on_guild_join] Dołączono do kanału {ch.name} (brak aktywnych członków) na serwerze {guild.name}")
                return
        print(f"[on_guild_join] Brak dostępnych kanałów głosowych z uprawnieniami na serwerze {guild.name}")
    except Exception as e:
        print(f"[on_guild_join] Błąd podczas próby dołączenia: {e}")

# ---------------- Shutdown handler ----------------
def _on_shutdown_signal(signum, frame):
    try:
        asyncio.get_event_loop().create_task(_save_and_close())
    except Exception:
        try:
            _save_cache_to_disk_sync(CACHE_FILE, search_cache)
        except Exception:
            pass

async def _save_and_close():
    try:
        await save_cache_to_disk()
    except Exception:
        await asyncio.to_thread(_save_cache_to_disk_sync, CACHE_FILE, search_cache)
    try:
        await bot.close()
    except Exception:
        pass

signal.signal(signal.SIGINT, _on_shutdown_signal)
signal.signal(signal.SIGTERM, _on_shutdown_signal)

# ---------------- Run ----------------
if __name__ == '__main__':
    if not FFMPEG_EXE.exists():
        print(f"Uwaga: nie znaleziono ffmpeg pod: {FFMPEG_EXE}")
        print("Spróbuję użyć ffmpeg z PATH. Jeśli to nie zadziała, ustaw poprawną ścieżkę w FFMPEG_EXE.")
    try:
        token = load_token(TOKEN_FILE)
    except Exception as e:
        print(f"Nie można wczytać tokenu: {e}")
        raise SystemExit(1)
    bot.run(token)