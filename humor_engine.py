"""
Humor Engine — PERELKI.NET Edition (SEKCJA 1)
Konfiguracja + globalny cache FIFO + pobieranie + parser perełki.net
"""

from __future__ import annotations
import os
import time
import json
import random
import hashlib
import threading
import re
import difflib
from typing import List, Dict, Any
from collections import deque, defaultdict

import requests
from bs4 import BeautifulSoup

# ============================================
# KONFIGURACJA
# ============================================

BASE_DIR = os.path.dirname(__file__)
CACHE_DIR = os.path.join(BASE_DIR, "humor_cache")
CACHE_FILE = os.path.join(CACHE_DIR, "global_cache.json")

USER_AGENT = "HumorEnginePerelkiBot/1.0 (+https://example.com)"
FETCH_TIMEOUT = 8
FETCH_RATE_SECONDS = 1.5

# Globalny cache max 1000 żartów (FIFO)
GLOBAL_CACHE_MAX = 1000

FUZZY_SIMILARITY_THRESHOLD = 0.86

NO_SOURCES_MESSAGE = (
    "Brak dostępnych perełek z dowcipami — spróbuj ponownie za chwilę."
)

PERELKI_RANDOM_URL = "https://perelki.net/random"
PERELKI_SOURCE_LABEL = "https://perelki.net/random"

# ============================================
# POMOCNICZE / NORMALIZACJA
# ============================================

_host_last_fetch = {}
_host_lock = threading.Lock()

def normalize(text: str) -> str:
    t = text.lower()
    t = t.replace("ó", "o").replace("ą", "a").replace("ę", "e")
    t = t.replace("ś", "s").replace("ć", "c").replace("ź", "z").replace("ż", "z").replace("ł", "l")
    t = re.sub(r"\s+", " ", t).strip()
    return t

def polite_get(url: str) -> requests.Response:
    """
    Pobiera stronę z delikatnym rate-limitem i obsługą redirectów.
    Zwraca obiekt Response (resp.text + resp.url).
    """
    with _host_lock:
        last = _host_last_fetch.get("perelki.net", 0)
        now = time.time()
        wait = FETCH_RATE_SECONDS - (now - last)
        if wait > 0:
            time.sleep(wait)
        _host_last_fetch["perelki.net"] = time.time()

    headers = {"User-Agent": USER_AGENT}
    print(f"[FETCH] GET {url}")
    resp = requests.get(url, headers=headers, timeout=FETCH_TIMEOUT, allow_redirects=True)
    resp.raise_for_status()
    return resp

def text_hash(text: str) -> str:
    return hashlib.sha256(text.strip().encode("utf-8")).hexdigest()

def is_similar_to_existing(text: str, existing_texts: List[str]) -> bool:
    for ex in existing_texts:
        ratio = difflib.SequenceMatcher(None, text, ex).ratio()
        if ratio >= FUZZY_SIMILARITY_THRESHOLD:
            return True
    return False

# ============================================
# GLOBALNY CACHE (FIFO)
# ============================================

if not os.path.isdir(CACHE_DIR):
    os.makedirs(CACHE_DIR, exist_ok=True)

def load_global_cache() -> Dict[str, Any]:
    if os.path.exists(CACHE_FILE):
        try:
            with open(CACHE_FILE, "r", encoding="utf-8") as f:
                raw = json.load(f)
            items = raw.get("items", [])
            return {
                "items": items,
                "hashes": set(it.get("hash") for it in items),
            }
        except Exception:
            return {"items": [], "hashes": set()}
    return {"items": [], "hashes": set()}

def save_global_cache(cache: Dict[str, Any]) -> None:
    serial = {
        "items": cache.get("items", []),
    }
    try:
        with open(CACHE_FILE, "w", encoding="utf-8") as f:
            json.dump(serial, f, ensure_ascii=False, indent=2)
    except Exception:
        pass

global_cache = load_global_cache()

def add_to_global_cache(texts: List[str]) -> int:
    """
    Dodaje nowe żarty do globalnego cache w trybie FIFO max GLOBAL_CACHE_MAX.
    """
    items = global_cache.get("items", [])
    hashes = global_cache.get("hashes", set())
    existing_texts = [it["text"] for it in items]

    added = 0
    for txt in texts:
        h = text_hash(txt)
        if h in hashes:
            continue
        if is_similar_to_existing(txt, existing_texts):
            continue

        items.append({
            "text": txt,
            "hash": h,
            "source": PERELKI_SOURCE_LABEL,
            "ts": time.time()
        })
        hashes.add(h)
        existing_texts.append(txt)
        added += 1

        # FIFO cutoff
        while len(items) > GLOBAL_CACHE_MAX:
            old = items.pop(0)
            oh = old.get("hash")
            if oh in hashes:
                hashes.remove(oh)

    global_cache["items"] = items
    global_cache["hashes"] = hashes

    if added:
        print(f"[CACHE] Globalnie dodano {added} żartów (łącznie: {len(items)})")

    save_global_cache(global_cache)
    return added

# ============================================
# PARSER PEREŁKI.NET
# ============================================

def extract_perelki_joke(html: str) -> List[str]:
    """
    Wyciąga żart z pierwszego <div class="container"> w sekcji <div class="content">.
    Usuwa <div class="about"> i zamienia <br> na nowe linie.
    """
    soup = BeautifulSoup(html, "html.parser")

    containers = soup.select("div.content > div.container")
    if not containers:
        print("[PARSE] Brak kontenerów z żartem")
        return []

    block = containers[0]

    # Zamień <br> na nowe linie
    for br in block.find_all("br"):
        br.replace_with("\n")

    # Usuń metadane
    about = block.find("div", class_="about")
    if about:
        about.decompose()

    text = block.get_text(separator=" ", strip=True)

    if len(text) < 10:
        return []

    print("[PARSE] perelki.net -> wyekstrahowano 1 żart")
    return [text]

# ============================================
# POBIERANIE ŻARTU
# ============================================

def fetch_one_joke_from_perelki() -> str | None:
    """
    Pobiera jeden losowy żart z perelki.net/random (z obsługą redirectów).
    """
    try:
        resp = polite_get(PERELKI_RANDOM_URL)
        html = resp.text
        jokes = extract_perelki_joke(html)
        if not jokes:
            print("[FETCH] Brak żartów w odpowiedzi perelki.net")
            return None
        return jokes[0]
    except Exception as e:
        print(f"[FETCH][ERROR] perelki.net -> {e}")
        return None

def ensure_cache_not_empty() -> None:
    """
    Jeśli cache pusty, pobierz kilka żartów z perelki.net/random.
    """
    if global_cache.get("items"):
        return

    print("[CACHE] Globalny cache pusty — inicjalizuję kilkoma żartami z perelki.net/random")
    for _ in range(5):
        joke = fetch_one_joke_from_perelki()
        if joke:
            add_to_global_cache([joke])

# ============================================
# PAMIĘĆ USERA
# ============================================

recent_jokes_global = deque(maxlen=200)

class UserState:
    def __init__(self):
        self.recent_jokes: deque = deque(maxlen=40)
        self.last_topic: str | None = None
        self.last_intent_type: str | None = None
        self.repeat_count: int = 0

user_states: Dict[str, UserState] = defaultdict(UserState)

# ============================================
# ANALIZA ZDAŃ / INTENCJI
# ============================================

HUMOR_TRIGGERS = [
    "dowcip", "kawał", "kawal", "żart", "zart",
    "śmieszne", "smieszne", "opowiedz", "powiedz coś śmiesznego",
    "opowiedz dowcip", "daj coś śmiesznego", "dawaj żart", "dawaj zarcik"
]

STORY_TRIGGERS = [
    "opowiedz historię", "opowiedz historie", "opowiedz coś ciekawego"
]

CONTINUE_TRIGGERS = [
    "powiedz coś jeszcze", "dawaj dalej", "jeszcze coś",
    "kontynuuj", "następny", "nastepny", "jeszcze jeden", "kolejny"
]

INSULT_TRIGGERS = [
    "głupi", "glupi", "idiota", "debil", "bezużyteczny", "bezuzyteczny"
]

COMPLIMENT_TRIGGERS = [
    "dobry bot", "fajny bot", "super bot", "dzięki", "dzieki"
]

QUESTION_WORDS = ["czy", "dlaczego", "czemu", "po co", "jak", "kiedy", "gdzie"]

STOPWORDS = {
    "z", "na", "do", "od", "ze", "po", "za", "dla", "o", "u", "w", "we",
    "i", "a", "że", "ze", "to", "ten", "ta", "tego", "tej", "tam", "tu",
    "czy", "dlaczego", "czemu", "jak", "gdzie", "kiedy", "po", "co",
    "jest", "sa", "są", "byl", "byla", "bylo"
}

def detect_is_question(text: str) -> bool:
    if text.strip().endswith("?"):
        return True
    for q in QUESTION_WORDS:
        if text.startswith(q + " ") or f" {q} " in text:
            return True
    return False

def extract_topic(text: str) -> str | None:
    """
    Minimalny ekstraktor tematu — nie wpływa na źródło,
    ale pozwala zachować kontekst dla 'kontynuuj'.
    """
    m = re.search(r"\bo ([a-zA-Ząćęłńóśźż]+)", text)
    if m and m.group(1).lower() not in STOPWORDS:
        return m.group(1).lower()

    words = re.findall(r"[a-zA-Ząćęłńóśźż]+", text)
    for w in reversed(words):
        lw = w.lower()
        if lw not in STOPWORDS and len(lw) > 2:
            return lw

    return None

def detect_intent(message: str, user: str) -> Dict[str, Any] | None:
    raw = message
    text = normalize(message)
    state = user_states[user]

    is_question = detect_is_question(text)
    topic = extract_topic(text)

    # Komplementy
    for c in COMPLIMENT_TRIGGERS:
        if c in text:
            return {
                "type": "compliment",
                "topic": topic,
                "raw": raw,
                "seriousness": "neutral"
            }

    # Obelgi
    for i in INSULT_TRIGGERS:
        if i in text:
            return {
                "type": "insult",
                "topic": topic,
                "raw": raw,
                "seriousness": "neutral"
            }

    # Kontynuacja
    for cont in CONTINUE_TRIGGERS:
        if cont in text:
            return {
                "type": "continue",
                "topic": state.last_topic,
                "raw": raw,
                "seriousness": state.last_intent_type or "neutral"
            }

    # Historia
    for s in STORY_TRIGGERS:
        if s in text:
            return {
                "type": "story",
                "topic": topic,
                "raw": raw,
                "seriousness": "neutral"
            }

    # Humor
    for h in HUMOR_TRIGGERS:
        if h in text:
            return {
                "type": "joke",
                "topic": topic,
                "raw": raw,
                "seriousness": "meme"
            }

    # Pytanie z tematem
    if is_question and topic:
        return {
            "type": "question_topic",
            "topic": topic,
            "raw": raw,
            "seriousness": "neutral"
        }

    # Pytanie ogólne
    if is_question:
        return {
            "type": "generic_question",
            "topic": topic,
            "raw": raw,
            "seriousness": "neutral"
        }

    return None

# ============================================
# WYBÓR ŻARTU Z GLOBALNEGO CACHE + DOPOBIERANIE
# ============================================

def choose_non_repeating_from_global(author: str) -> str | None:
    """
    Wybiera żart z globalnego cache, starając się unikać powtórek
    (per user + globalnie).
    """
    items = global_cache.get("items", [])
    if not items:
        return None

    state = user_states[author]
    pool = [it["text"] for it in items]

    random.shuffle(pool)
    for candidate in pool:
        if candidate in state.recent_jokes:
            continue
        if candidate in recent_jokes_global:
            continue
        return candidate

    # jeśli wszystko już było — trudno, weź losowy
    return random.choice(pool)

def get_joke(author: str) -> str:
    """
    Główna logika pobierania żartu:
    - jeśli cache pusty -> doładowanie z perelki.net/random
    - wybór niepowtarzającego się żartu
    - jeśli nadal słabo -> pobierz nowy żart i dorzuć do cache
    """
    ensure_cache_not_empty()

    state = user_states[author]
    state.repeat_count += 1

    chosen_text = choose_non_repeating_from_global(author)

    if not chosen_text:
        # nic w cache — spróbuj pobrać coś nowego
        joke = fetch_one_joke_from_perelki()
        if joke:
            add_to_global_cache([joke])
            chosen_text = joke
        else:
            return NO_SOURCES_MESSAGE

    # dodatkowy anti‑repeat: jeśli mimo wszystko trafiliśmy w niedawny żart
    if chosen_text in state.recent_jokes or chosen_text in recent_jokes_global:
        alt = fetch_one_joke_from_perelki()
        if alt:
            add_to_global_cache([alt])
            chosen_text = alt

    answer_text = chosen_text
    if state.repeat_count >= 3:
        meta = f"(powtórzenie #{state.repeat_count})"
        answer_text = f"{chosen_text} {meta}"

    final = answer_text

    state.recent_jokes.append(chosen_text)
    recent_jokes_global.append(chosen_text)

    print(f"[JOKE] Zwrócono żart (użytkownik: {author})")
    return final

# ============================================
# GŁÓWNA FUNKCJA + ALIAS
# ============================================

def get_humor_for_message(message: str, author: str) -> str | None:
    """
    Główne wejście dla bota.
    Zwraca string z żartem lub None, jeśli nie trzeba reagować humorem.
    """
    text = message or ""
    norm = normalize(text)
    state = user_states[author]

    intent = detect_intent(text, author)

    if intent:
        ttype = intent["type"]
        topic = intent.get("topic")
        raw = intent.get("raw", text)

        state.last_intent_type = ttype
        if topic:
            state.last_topic = topic

        if ttype == "compliment":
            return random.choice([
                "Dzięki — zapisuję to w pamięci RAM‑u.",
                "Miło słyszeć. W zamian mogę opowiedzieć perełkę z dowcipem.",
                "Doceniam! Jeśli chcesz, losuję coś z perelek."
            ])

        if ttype == "insult":
            return random.choice([
                "Obrażanie bota to jak kłótnia z mikrofalówką — ona i tak będzie robić swoje.",
                "Twoja obelga została zapisana w logach. Nic z niej nie wynika.",
                "Jeśli to miało mnie zranić, to muszę Cię rozczarować — jestem z krzemu."
            ])

        if ttype == "continue":
            return get_joke(author)

        if ttype in ("joke", "question_topic", "situational", "story"):
            return get_joke(author)

        if ttype == "generic_question":
            return random.choice([
                "Na to pytanie nie mam gotowej odpowiedzi, ale mogę Ci wylosować dowcip.",
                "Nie mam encyklopedii, mam perełki z dowcipami. Chcesz żart?",
                "Brzmi poważnie — a ja mam tylko humor. Napisz: 'opowiedz dowcip'."
            ])

    # fallback: jeśli w treści jest wyraźne słowo‑wyzwalacz
    for h in HUMOR_TRIGGERS:
        if h in norm:
            return get_joke(author)

    # jeśli nic nie wskazuje na humor — milczymy
    return None

def humor_handle(message: str, author: str) -> str | None:
    return get_humor_for_message(message, author)
