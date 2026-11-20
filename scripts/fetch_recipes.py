#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
KitchenMaster High-Throughput Async Recipe Pipeline & POST GTX Translation
Скрипт сбора, нормализации, перевода через Google Translate GTX и разбивки
на ежедневные дельта-чанки (data/daily/YYYY-MM-DD.json).
Включает строгий классификатор категорий (защита от попадания блинов/выпечки в Салаты)
и локализацию кулинарных единиц измерения.
"""

import asyncio
import concurrent.futures
import hashlib
import html
import json
import os
import re
import time
import urllib.parse
import urllib.request
from datetime import datetime, timezone

try:
    import aiohttp
    AIOHTTP_AVAILABLE = True
except ImportError:
    AIOHTTP_AVAILABLE = False

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(PROJECT_ROOT, "data")
DAILY_DIR = os.path.join(DATA_DIR, "daily")
SEED_FILE = os.path.join(PROJECT_ROOT, "app", "src", "main", "assets", "recipes_seed.json")
FEED_FILE = os.path.join(DATA_DIR, "recipes_feed.json")
MANIFEST_FILE = os.path.join(DATA_DIR, "manifest.json")
SEEN_IDS_FILE = os.path.join(DATA_DIR, "seen_ids.json")
TRANSLATION_CACHE_FILE = os.path.join(DATA_DIR, "translation_cache.json")

TARGET_RU_PER_RUN = 150
TARGET_EN_PER_RUN = 150
MAX_DAILY_CHUNKS_KEEP = 14
FEED_CAP_RECIPES = 300
CONCURRENCY_LIMIT = 10

GTX_URL = "https://translate.googleapis.com/translate_a/single?client=gtx&sl=auto&tl=ru&dt=t"
HTTP_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}

PANTRY_STAPLES = {
    "вода", "соль", "сахар", "перец", "черный перец", "молотый перец",
    "масло растительное", "растительное масло", "подсолнечное масло",
    "оливковое масло", "масло подсолнечное", "соль по вкусу", "перец по вкусу"
}

NORMALIZATION_MAP = {
    "помидор": "помидоры", "помидоры": "помидоры", "томат": "помидоры", "томаты": "помидоры", "черри": "помидоры",
    "яйцо": "яйца", "яйца": "яйца", "яичный желток": "яйца", "желток": "яйца", "куриное яйцо": "яйца",
    "картошка": "картофель", "картофель": "картофель",
    "курица": "курица", "куриное филе": "курица", "куриная грудка": "курица", "филе куриное": "курица",
    "лук": "лук", "репчатый лук": "лук", "лук репчатый": "лук", "зеленый лук": "лук", "красный лук": "лук",
    "морковь": "морковь",
    "сыр": "сыр", "пармезан": "сыр", "моцарелла": "сыр", "фета": "сыр", "сулугуни": "сыр",
    "молоко": "молоко", "сметана": "сметана", "сливки": "сливки", "творог": "творог",
    "чеснок": "чеснок", "рис": "рис", "макароны": "макароны", "паста": "макароны", "спагетти": "макароны",
    "говядина": "говядина", "свинина": "свинина",
    "лосось": "лосось", "рыба": "лосось", "семга": "лосось", "форель": "лосось", "креветки": "креветки",
    "шампиньоны": "шампиньоны", "грибы": "шампиньоны",
    "огурцы": "огурцы", "огурец": "огурцы", "баклажан": "баклажан", "кабачок": "кабачок", "кабачки": "кабачок",
    "мука": "мука", "пшеничная мука": "мука", "яблоки": "яблоки", "яблоко": "яблоки", "шоколад": "шоколад"
}

# --- Кэш переводов ---

_translation_cache = {}

def load_translation_cache():
    global _translation_cache
    if os.path.exists(TRANSLATION_CACHE_FILE):
        try:
            with open(TRANSLATION_CACHE_FILE, "r", encoding="utf-8") as f:
                _translation_cache = json.load(f)
            print(f"[INFO] Loaded {len(_translation_cache)} entries from translation cache.")
        except Exception:
            _translation_cache = {}

def save_translation_cache():
    with open(TRANSLATION_CACHE_FILE, "w", encoding="utf-8") as f:
        json.dump(_translation_cache, f, ensure_ascii=False)
    print(f"[INFO] Saved {len(_translation_cache)} entries to {TRANSLATION_CACHE_FILE}")

def translate_gtx(text: str) -> str:
    """Выполняет POST запрос к Google Translate (GTX) с кэшированием и защитой от 429."""
    if not text or not text.strip():
        return text
    clean_text = text.strip()
    cache_key = hashlib.md5(clean_text.encode("utf-8")).hexdigest()
    if cache_key in _translation_cache:
        return _translation_cache[cache_key]

    data = urllib.parse.urlencode({"q": clean_text}).encode("utf-8")
    req = urllib.request.Request(GTX_URL, data=data, headers=HTTP_HEADERS)
    for attempt in range(4):
        try:
            with urllib.request.urlopen(req, timeout=12) as resp:
                raw_json = json.loads(resp.read().decode("utf-8"))
                translated = "".join([part[0] for part in raw_json[0] if part[0]])
                _translation_cache[cache_key] = translated
                time.sleep(0.35)
                return translated
        except Exception as e:
            if "429" in str(e):
                backoff = 2.0 * (attempt + 1)
                time.sleep(backoff)
            else:
                print(f"[WARN] GTX translation failed: {e}")
                break
    return clean_text

def localize_units(amount: str) -> str:
    """Локализует кулинарные меры измерения на русский язык."""
    s = amount.strip()
    replacements = [
        (r"(?i)\b(\d+[\d\s.,/-]*)\s*cups?\b", r"\1 стак."),
        (r"(?i)\bcups?\b", "стак."),
        (r"(?i)\b(\d+[\d\s.,/-]*)\s*(?:tablespoons?|tbsp?|tbs)\b", r"\1 ст. л."),
        (r"(?i)\b(?:tablespoons?|tbsp?|tbs)\b", "ст. л."),
        (r"(?i)\b(\d+[\d\s.,/-]*)\s*(?:teaspoons?|tsp)\b", r"\1 ч. л."),
        (r"(?i)\b(?:teaspoons?|tsp)\b", "ч. л."),
        (r"(?i)\bpinch(?:es)?\b", "щепотка"),
        (r"(?i)\bto taste\b", "по вкусу"),
        (r"(?i)\bfor garnish\b|\bgarnish\b|\bдля украшения\b", "для подачи"),
        (r"(?i)\bcloves?\b", "зубч."),
        (r"(?i)\bounces?\b|\boz\b", "унц."),
        (r"(?i)\bpounds?\b|\blbs?\b", "фунт."),
        (r"(?i)\bgrams?\b|\bg\b", "г"),
        (r"(?i)\bml\b", "мл"),
        (r"(?i)\bl\b", "л"),
        (r"\b1 ст\.\b", "1 ст. л.")
    ]
    for pattern, repl in replacements:
        s = re.sub(pattern, repl, s)
    return s.strip()

def normalize_ingredient(name: str) -> str:
    clean = name.strip().lower()
    clean = re.sub(r"[^а-яa-z0-9\s-]", "", clean).strip()
    return NORMALIZATION_MAP.get(clean, clean)

def parse_iso_duration(duration_str: str) -> int:
    if not duration_str:
        return 35
    match = re.search(r"PT(?:(\d+)H)?(?:(\d+)M)?", duration_str)
    if match:
        h = int(match.group(1) or 0)
        m = int(match.group(2) or 0)
        total = h * 60 + m
        return total if total > 5 else 35
    digits = re.findall(r"\d+", duration_str)
    if digits:
        return max(10, min(180, int(digits[0])))
    return 35

def split_russian_ingredient(full_str: str) -> tuple:
    full_str = full_str.strip()
    parts = re.split(r"[-—–]\s*|\s*,\s*(?=\d)", full_str)
    if len(parts) >= 2:
        return parts[0].strip(), localize_units(parts[1].strip())
    match = re.search(r"^(.*?)\s+(\d+[\d\s.,/-]*\s*(?:г|кг|мл|л|ст\.?\s*л\.?|ч\.?\s*л\.?|шт\.?|зубч\.?|по вкусу|щепотка).*)$", full_str, re.IGNORECASE)
    if match:
        return match.group(1).strip(), localize_units(match.group(2).strip())
    return full_str, "по вкусу"

# --- Строгий классификатор категорий (защита от пересорта в Салаты) ---

def classify_strict_category(title: str, desc: str = "", orig_category: str = "") -> str:
    combined = f"{title} {desc}".lower()
    cat_lower = (orig_category or "").lower()

    # 1. СУПЫ И ПЕРВЫЕ БЛЮДА (FIRST)
    first_keywords = [
        "soup", "stew", "chowder", "broth", "ramen", "pho", "borscht",
        "суп", "борщ", "щи", "солянка", "уха", "окрошка", "рассольник", "бульон", "гаспачо"
    ]
    for k in first_keywords:
        if (len(k) <= 4 and re.search(rf"\b{k}\b", combined)) or (len(k) > 4 and k in combined):
            return "FIRST"

    # 2. ЗАВТРАКИ (BREAKFAST)
    breakfast_keywords = [
        "pancake", "pancakes", "blini", "waffle", "waffles", "omelet", "omelette",
        "scrambled", "porridge", "oatmeal", "granola", "toast", "crepe", "crepes",
        "syirniki", "syrniki", "блины", "оладьи", "сырники", "каша", "омлет", "яичница",
        "завтрак", "тост", "гранола"
    ]
    for k in breakfast_keywords:
        if (len(k) <= 4 and re.search(rf"\b{k}\b", combined)) or (len(k) > 4 and k in combined):
            return "BREAKFAST"
    if cat_lower == "breakfast":
        return "BREAKFAST"

    # 3. ДЕСЕРТЫ И ВЫПЕЧКА (DESSERT)
    dessert_keywords = [
        "cake", "pie", "tart", "cookie", "cookies", "brownie", "brownies", "pudding",
        "mousse", "tiramisu", "cheesecake", "brioche", "pastry", "pastries", "sweet",
        "chocolate", "dessert", "muffin", "muffins", "cupcake", "cupcakes", "ice cream",
        "торт", "пирог", "печенье", "десерт", "чизкейк", "тирамису", "шарлотка", "выпечка",
        "кекс", "маффин", "брауни", "крем", "мороженое", "эклеры"
    ]
    for k in dessert_keywords:
        if (len(k) <= 4 and re.search(rf"\b{k}\b", combined)) or (len(k) > 4 and k in combined):
            return "DESSERT"
    if cat_lower == "dessert":
        return "DESSERT"

    # 4. САЛАТЫ (SALAD) — СТРОГОЕ СОВПАДЕНИЕ ТОЛЬКО В НАЗВАНИИ ИЛИ ОПИСАНИИ!
    salad_keywords = [
        "salad", "salads", "slaw", "coleslaw", "tabbouleh", "ceviche",
        "салат", "винегрет", "оливье", "цезарь", "греческий"
    ]
    for k in salad_keywords:
        if (len(k) <= 4 and re.search(rf"\b{k}\b", combined)) or (len(k) > 4 and k in combined):
            return "SALAD"

    # 5. ВТОРЫЕ БЛЮДА (SECOND) — все остальные основные блюда
    return "SECOND"

def deduplicate_ingredients(items: list) -> list:
    if not items:
        return []
    seen = {}
    display_names = {}
    for item in items:
        name = item.get("name", "").strip()
        amt = item.get("amount", "").strip()
        if not name:
            continue
        key = name.lower()
        if key not in seen:
            seen[key] = []
            display_names[key] = name
        if amt:
            seen[key].append(amt)

    deduped = []
    for key, amounts in seen.items():
        name = display_names[key]
        if not amounts:
            combined_amount = ""
        elif len(amounts) == 1:
            combined_amount = amounts[0]
        else:
            combined_amount = " + ".join(amounts)
        deduped.append({"name": name, "amount": combined_amount})
    return deduped

def build_recipe_dict(
    recipe_id: str,
    title_ru: str,
    title_orig: str,
    desc_ru: str,
    desc_orig: str,
    category: str,
    cook_time: int,
    servings: int,
    image_url: str,
    source_lang: str,
    ingredients_orig: list,
    ingredients_ru: list,
    normalized_ingredients: set,
    steps_orig: list,
    steps_ru: list
) -> dict:
    ingredients_ru = deduplicate_ingredients(ingredients_ru)
    ingredients_orig = deduplicate_ingredients(ingredients_orig)

    allergens = []
    norm_list = list(normalized_ingredients)
    if any(i in norm_list for i in ["молоко", "сыр", "сливки", "сметана", "творог"]):
        allergens.append("lactose")
    if any(i in norm_list for i in ["мука", "макароны", "хлеб"]):
        allergens.append("gluten")
    if any(i in norm_list for i in ["яйца"]):
        allergens.append("egg")
    if any(i in norm_list for i in ["лосось", "креветки", "рыба"]):
        allergens.append("seafood")

    return {
        "id": recipe_id,
        "titleRu": title_ru,
        "titleOrig": title_orig,
        "descriptionRu": desc_ru if desc_ru and len(desc_ru) > 10 else f"Ароматное блюдо: {title_ru}.",
        "descriptionOrig": desc_orig if desc_orig and len(desc_orig) > 10 else f"Dish: {title_orig}.",
        "category": category,
        "cookTimeMinutes": cook_time,
        "servings": servings,
        "imageUrl": image_url if image_url else None,
        "sourceLang": source_lang,
        "isFavorite": False,
        "ingredientsRu": ingredients_ru,
        "ingredientsOrig": ingredients_orig,
        "normalizedIngredients": sorted(list(normalized_ingredients)),
        "stepsRu": steps_ru,
        "stepsOrig": steps_orig,
        "allergens": allergens
    }

# --- Полный пакетный перевод англоязычных рецептов через POST GTX ---

def translate_english_recipe(title_orig: str, desc_orig: str, ingredients_orig: list, steps_orig: list) -> tuple:
    """Переводит рецепт пакетным POST вызовом, структурируя секции."""
    sec_delim = "\n\n===SEC===\n\n"
    item_delim = "\n---\n"
    pair_delim = " ::: "

    ings_block = item_delim.join([f"{ing['name']}{pair_delim}{ing['amount']}" for ing in ingredients_orig])
    steps_block = item_delim.join(steps_orig)

    payload = f"{title_orig}{sec_delim}{desc_orig}{sec_delim}{ings_block}{sec_delim}{steps_block}"
    translated = translate_gtx(payload)

    sections = re.split(r"\n*(?:===SEC===|===РАЗДЕЛ===)\n*", translated)

    title_ru = sections[0].strip().replace("(Шеф-рецепт)", "").strip() if len(sections) > 0 else title_orig
    desc_ru = sections[1].strip() if len(sections) > 1 else f"Блюдо: {title_ru}."

    ingredients_ru = []
    normalized_ingredients = set()

    if len(sections) > 2 and sections[2].strip():
        raw_ings = sections[2].strip().split(item_delim)
        for line in raw_ings:
            line = line.strip()
            if not line:
                continue
            parts = line.split(pair_delim)
            if len(parts) >= 2:
                name = parts[0].strip()
                amount = localize_units(parts[1].strip())
            else:
                name = line
                amount = "по вкусу"
            ingredients_ru.append({"name": name, "amount": amount})
            norm = normalize_ingredient(name)
            if norm and norm.lower() not in PANTRY_STAPLES and len(norm) > 2:
                normalized_ingredients.add(norm)
    else:
        for ing in ingredients_orig:
            ingredients_ru.append({"name": ing["name"], "amount": localize_units(ing["amount"])})

    steps_ru = []
    if len(sections) > 3 and sections[3].strip():
        raw_steps = sections[3].strip().split(item_delim)
        steps_ru = [s.strip() for s in raw_steps if s.strip()]
    else:
        steps_ru = steps_orig

    return title_ru, desc_ru, ingredients_ru, normalized_ingredients, steps_ru

# --- Сбор TheMealDB с POST GTX переводом ---

def parse_meal(meal: dict) -> dict:
    meal_id = f"mealdb-{meal.get('idMeal')}"
    title_orig = meal.get("strMeal", "").strip()
    category_raw = meal.get("strCategory", "Miscellaneous")
    instructions = meal.get("strInstructions", "").strip()
    image_url = meal.get("strMealThumb", "")

    # Очищаем шаги на английском
    raw_steps = [s.strip() for s in re.split(r"\r?\n|\.\s+", instructions) if len(s.strip()) > 10]
    steps_orig = raw_steps[:8] if raw_steps else [instructions]

    ingredients_orig = []
    for i in range(1, 21):
        ing = meal.get(f"strIngredient{i}")
        measure = meal.get(f"strMeasure{i}")
        if ing and ing.strip():
            ingredients_orig.append({
                "name": ing.strip(),
                "amount": measure.strip() if measure else "to taste"
            })

    desc_orig = f"Traditional {category_raw} dish: {title_orig} with balanced flavors and presentation."

    # Строгая классификация категории
    category = classify_strict_category(title_orig, desc_orig, category_raw)

    # Высококачественный POST GTX перевод
    title_ru, desc_ru, ingredients_ru, normalized_ingredients, steps_ru = translate_english_recipe(
        title_orig, desc_orig, ingredients_orig, steps_orig
    )

    return build_recipe_dict(
        recipe_id=meal_id,
        title_ru=title_ru,
        title_orig=title_orig,
        desc_ru=desc_ru,
        desc_orig=desc_orig,
        category=category,
        cook_time=35,
        servings=4,
        image_url=image_url,
        source_lang="EN",
        ingredients_orig=ingredients_orig,
        ingredients_ru=ingredients_ru,
        normalized_ingredients=normalized_ingredients,
        steps_orig=steps_orig,
        steps_ru=steps_ru
    )

def http_get_sync(url: str, timeout: int = 10, encoding: str = "utf-8") -> str:
    req = urllib.request.Request(url, headers=HTTP_HEADERS)
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return resp.read().decode(encoding, errors="ignore")

def fetch_themealdb_batch(letters: list, seen_ids: set, target_count: int) -> list:
    print(f"[INFO] Fetching international recipes from TheMealDB (target: {target_count})...")
    recipes = []
    for letter in letters:
        if len(recipes) >= target_count:
            break
        url = f"https://www.themealdb.com/api/json/v1/1/search.php?f={letter}"
        try:
            content = http_get_sync(url, timeout=10)
            data = json.loads(content)
            meals = data.get("meals") or []
            for m in meals:
                mid = f"mealdb-{m.get('idMeal')}"
                if mid in seen_ids:
                    continue
                parsed = parse_meal(m)
                if parsed:
                    recipes.append(parsed)
                    seen_ids.add(mid)
                    if len(recipes) >= target_count:
                        break
        except Exception as e:
            print(f"[WARN] Error fetching TheMealDB letter {letter}: {e}")
        time.sleep(0.1)

    print(f"[SUCCESS] Collected {len(recipes)} unique translated international recipes from TheMealDB.")
    return recipes

# --- Сбор русских рецептов (Povarenok.ru Microdata + Eda.ru Schema.org) ---

def parse_povarenok_page(html_text: str, rid: str, link: str) -> dict:
    name_m = re.search(r"<h1[^>]*itemprop=[\"\x27]name[\"\x27][^>]*>(.*?)</h1>", html_text, re.DOTALL)
    if not name_m:
        return None
    title = html.unescape(name_m.group(1).strip())

    desc_m = re.search(r"<div[^>]*itemprop=[\"\x27]description[\"\x27][^>]*>(.*?)</div>", html_text, re.DOTALL)
    desc = html.unescape(re.sub(r"<[^>]+>", " ", desc_m.group(1)).strip()) if desc_m else f"Традиционный домашний рецепт: {title}."

    img_m = re.search(r"<img[^>]*itemprop=[\"\x27]image[\"\x27][^>]*src=[\"\x27](.*?)[\"\x27]", html_text)
    img_url = img_m.group(1) if img_m else ""

    time_m = re.search(r"itemprop=[\"\x27](?:cookTime|totalTime)[\"\x27][^>]*datetime=[\"\x27](.*?)[\"\x27]", html_text) or re.search(r"itemprop=[\"\x27](?:cookTime|totalTime)[\"\x27][^>]*>(.*?)</", html_text)
    cook_time = parse_iso_duration(time_m.group(1) if time_m else "")

    category = classify_strict_category(title, desc)

    raw_ings = re.findall(r"<li[^>]*itemprop=[\"\x27]recipeIngredient[\"\x27][^>]*>(.*?)</li>", html_text, re.DOTALL)
    if not raw_ings:
        return None

    ingredients_orig = []
    ingredients_ru = []
    normalized_ingredients = set()

    for r_ing in raw_ings:
        clean_ing = " ".join(html.unescape(re.sub(r"<[^>]+>", " ", r_ing)).split())
        name, amount = split_russian_ingredient(clean_ing)
        ingredients_orig.append({"name": name, "amount": amount})
        ingredients_ru.append({"name": name, "amount": amount})
        norm = normalize_ingredient(name)
        if norm and norm.lower() not in PANTRY_STAPLES and len(norm) > 2:
            normalized_ingredients.add(norm)

    steps_block = re.search(r"<ul[^>]*itemprop=[\"\x27]recipeInstructions[\"\x27][^>]*>(.*?)</ul>", html_text, re.DOTALL)
    steps = []
    if steps_block:
        for s in re.findall(r"<li[^>]*>(.*?)</li>", steps_block.group(1), re.DOTALL):
            clean_s = " ".join(html.unescape(re.sub(r"<[^>]+>", " ", s)).split())
            if len(clean_s) > 10:
                steps.append(clean_s)
    if not steps:
        steps = [f"Приготовить блюдо «{title}» в соответствии с пропорциями ингредиентов и подать к столу."]

    return build_recipe_dict(
        recipe_id=f"povarenok-{rid}",
        title_ru=title,
        title_orig=title,
        desc_ru=desc,
        desc_orig=desc,
        category=category,
        cook_time=cook_time,
        servings=4,
        image_url=img_url,
        source_lang="RU",
        ingredients_orig=ingredients_orig,
        ingredients_ru=ingredients_ru,
        normalized_ingredients=normalized_ingredients,
        steps_orig=steps,
        steps_ru=steps
    )

def fetch_russian_recipes_povarenok(seen_ids: set, target_count: int) -> list:
    print(f"[INFO] Fetching Russian recipes from povarenok.ru (target: {target_count})...")
    recipes = []
    page = 1
    max_pages = 25

    recipe_links_to_fetch = []

    while page <= max_pages and len(recipe_links_to_fetch) < target_count * 2:
        url = f"https://www.povarenok.ru/recipes/~{page}/" if page > 1 else "https://www.povarenok.ru/recipes/"
        try:
            p_html = http_get_sync(url, timeout=10, encoding="windows-1251")
            links = set(re.findall(r"href=[\"\x27](https://www.povarenok.ru/recipes/show/(\d+)/)[\"\x27]", p_html))
            for link, rid in links:
                recipe_id = f"povarenok-{rid}"
                if recipe_id not in seen_ids and (link, rid) not in recipe_links_to_fetch:
                    recipe_links_to_fetch.append((link, rid))
        except Exception as e:
            print(f"[WARN] Error fetching povarenok page {page}: {e}")
        page += 1

    print(f"[INFO] Found {len(recipe_links_to_fetch)} candidate recipe links on povarenok.ru")

    def fetch_single(item):
        link, rid = item
        try:
            html_text = http_get_sync(link, timeout=10, encoding="windows-1251")
            return parse_povarenok_page(html_text, rid, link)
        except Exception:
            return None

    with concurrent.futures.ThreadPoolExecutor(max_workers=CONCURRENCY_LIMIT) as executor:
        futures = {executor.submit(fetch_single, item): item for item in recipe_links_to_fetch}
        for future in concurrent.futures.as_completed(futures):
            parsed = future.result()
            if parsed and parsed["id"] not in seen_ids:
                seen_ids.add(parsed["id"])
                recipes.append(parsed)
                if len(recipes) >= target_count:
                    break

    print(f"[SUCCESS] Collected {len(recipes)} unique Russian recipes from povarenok.ru.")
    return recipes

def fetch_russian_recipes_eda(seen_ids: set, target_count: int) -> list:
    print(f"[INFO] Fetching Russian recipes from eda.ru (Schema.org JSON-LD, target: {target_count})...")
    recipes = []
    page = 1
    max_pages = 10

    while len(recipes) < target_count and page <= max_pages:
        url = f"https://eda.ru/recepty?page={page}"
        try:
            p_html = http_get_sync(url, timeout=10)
            scripts = re.findall(r'<script[^>]*type=[\"\x27]application/ld\+json[\"\x27][^>]*>(.*?)</script>', p_html, re.DOTALL)
            for s in scripts:
                try:
                    data = json.loads(s.strip())
                    items = data if isinstance(data, list) else [data]
                    for item in items:
                        if isinstance(item, dict) and item.get("@type") == "Recipe":
                            name = item.get("name")
                            if not name:
                                continue
                            clean_name = html.unescape(name).strip()
                            clean_id = f"eda-{abs(hash(clean_name)) % 1000000}"
                            if clean_id in seen_ids:
                                continue

                            desc = item.get("description", f"Рецепт: {clean_name}")
                            cook_time = parse_iso_duration(item.get("cookTime") or item.get("totalTime") or "")
                            category = classify_strict_category(clean_name, desc)

                            raw_ings = item.get("recipeIngredient", [])
                            ingredients_orig = []
                            ingredients_ru = []
                            normalized_ingredients = set()

                            for r_ing in raw_ings:
                                r_clean = html.unescape(str(r_ing)).strip()
                                ing_name, amount = split_russian_ingredient(r_clean)
                                ingredients_orig.append({"name": ing_name, "amount": amount})
                                ingredients_ru.append({"name": ing_name, "amount": amount})
                                norm = normalize_ingredient(ing_name)
                                if norm and norm.lower() not in PANTRY_STAPLES and len(norm) > 2:
                                    normalized_ingredients.add(norm)

                            steps_data = item.get("recipeInstructions", [])
                            steps = []
                            if isinstance(steps_data, list):
                                for st in steps_data:
                                    if isinstance(st, dict):
                                        text = st.get("text", "")
                                    else:
                                        text = str(st)
                                    text_clean = html.unescape(re.sub(r"<[^>]+>", " ", text)).strip()
                                    if len(text_clean) > 8:
                                        steps.append(text_clean)
                            elif isinstance(steps_data, str):
                                steps = [html.unescape(re.sub(r"<[^>]+>", " ", steps_data)).strip()]

                            if not steps:
                                steps = [f"Приготовить «{clean_name}» согласно рецептуре и подать к столу."]

                            img = item.get("image")
                            img_url = ""
                            if isinstance(img, list) and img:
                                img_url = img[0]
                            elif isinstance(img, str):
                                img_url = img

                            parsed = build_recipe_dict(
                                recipe_id=clean_id,
                                title_ru=clean_name,
                                title_orig=clean_name,
                                desc_ru=desc,
                                desc_orig=desc,
                                category=category,
                                cook_time=cook_time,
                                servings=4,
                                image_url=img_url,
                                source_lang="RU",
                                ingredients_orig=ingredients_orig,
                                ingredients_ru=ingredients_ru,
                                normalized_ingredients=normalized_ingredients,
                                steps_orig=steps,
                                steps_ru=steps
                            )
                            seen_ids.add(clean_id)
                            recipes.append(parsed)
                            if len(recipes) >= target_count:
                                break
                except Exception:
                    pass
        except Exception as e:
            print(f"[WARN] Error fetching eda.ru page {page}: {e}")
        page += 1

    print(f"[SUCCESS] Collected {len(recipes)} unique Russian recipes from eda.ru.")
    return recipes

# --- Хранилище дельта-чанков и дедупликация ---

def load_seen_ids() -> set:
    seen = set()
    if os.path.exists(SEEN_IDS_FILE):
        try:
            with open(SEEN_IDS_FILE, "r", encoding="utf-8") as f:
                seen = set(json.load(f))
            print(f"[INFO] Loaded {len(seen)} existing seen IDs from {SEEN_IDS_FILE}")
            return seen
        except Exception as e:
            print(f"[WARN] Could not load {SEEN_IDS_FILE}: {e}")

    if os.path.exists(SEED_FILE):
        try:
            with open(SEED_FILE, "r", encoding="utf-8") as f:
                for r in json.load(f):
                    seen.add(r["id"])
        except Exception:
            pass

    if os.path.exists(FEED_FILE):
        try:
            with open(FEED_FILE, "r", encoding="utf-8") as f:
                for r in json.load(f):
                    seen.add(r["id"])
        except Exception:
            pass

    if os.path.exists(DAILY_DIR):
        for fname in os.listdir(DAILY_DIR):
            if fname.endswith(".json"):
                fpath = os.path.join(DAILY_DIR, fname)
                try:
                    with open(fpath, "r", encoding="utf-8") as f:
                        for r in json.load(f):
                            seen.add(r["id"])
                except Exception:
                    pass

    return seen

def save_seen_ids(seen_ids: set):
    with open(SEEN_IDS_FILE, "w", encoding="utf-8") as f:
        json.dump(sorted(list(seen_ids)), f, ensure_ascii=False)
    print(f"[INFO] Saved {len(seen_ids)} total seen IDs to {SEEN_IDS_FILE}")

def save_daily_chunk(new_recipes: list, today_str: str) -> str:
    os.makedirs(DAILY_DIR, exist_ok=True)
    chunk_path = os.path.join(DAILY_DIR, f"{today_str}.json")

    existing_recipes = {}
    if os.path.exists(chunk_path):
        try:
            with open(chunk_path, "r", encoding="utf-8") as f:
                for r in json.load(f):
                    existing_recipes[r["id"]] = r
        except Exception as e:
            print(f"[WARN] Could not parse existing chunk {chunk_path}: {e}")

    for r in new_recipes:
        existing_recipes[r["id"]] = r

    final_chunk = list(existing_recipes.values())
    with open(chunk_path, "w", encoding="utf-8") as f:
        json.dump(final_chunk, f, ensure_ascii=False, indent=2)

    print(f"[SUCCESS] Wrote {len(final_chunk)} recipes to daily chunk: {chunk_path}")
    return chunk_path

def prune_old_daily_chunks(max_keep: int = MAX_DAILY_CHUNKS_KEEP) -> list:
    if not os.path.exists(DAILY_DIR):
        return []
    all_chunks = []
    for fname in os.listdir(DAILY_DIR):
        if fname.endswith(".json") and re.match(r"^\d{4}-\d{2}-\d{2}\.json$", fname):
            date_part = fname.replace(".json", "")
            all_chunks.append((date_part, fname))

    all_chunks.sort(key=lambda x: x[0], reverse=True)
    retained = all_chunks[:max_keep]
    pruned = all_chunks[max_keep:]

    for date_part, fname in pruned:
        fpath = os.path.join(DAILY_DIR, fname)
        try:
            os.remove(fpath)
            print(f"[INFO] Pruned old daily chunk: {fname}")
        except Exception as e:
            print(f"[WARN] Failed to remove {fname}: {e}")

    return retained

def update_manifest_and_feed(retained_chunks: list):
    chunks_meta = []
    total_in_chunks = 0
    recent_recipes_for_feed = []

    for date_part, fname in retained_chunks:
        fpath = os.path.join(DAILY_DIR, fname)
        try:
            with open(fpath, "r", encoding="utf-8") as f:
                content = f.read()
                recipes = json.loads(content)
            sha256 = hashlib.sha256(content.encode("utf-8")).hexdigest()
            count = len(recipes)
            total_in_chunks += count

            chunks_meta.append({
                "date": date_part,
                "url": f"https://raw.githubusercontent.com/Outrot174/KitchenMasterRecipes/main/data/daily/{fname}",
                "sha256": sha256,
                "count": count
            })

            if len(recent_recipes_for_feed) < FEED_CAP_RECIPES:
                for r in recipes:
                    if len(recent_recipes_for_feed) < FEED_CAP_RECIPES:
                        recent_recipes_for_feed.append(r)
        except Exception as e:
            print(f"[WARN] Error reading chunk {fpath}: {e}")

    chunks_meta.sort(key=lambda x: x["date"], reverse=True)

    feed_json = json.dumps(recent_recipes_for_feed, ensure_ascii=False, indent=2)
    with open(FEED_FILE, "w", encoding="utf-8") as f:
        f.write(feed_json)
    feed_sha256 = hashlib.sha256(feed_json.encode("utf-8")).hexdigest()

    current_version = 10
    if os.path.exists(MANIFEST_FILE):
        try:
            with open(MANIFEST_FILE, "r", encoding="utf-8") as f:
                prev_manifest = json.load(f)
                current_version = prev_manifest.get("version", 10) + 1
        except Exception:
            pass

    now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    manifest = {
        "version": current_version,
        "updatedAt": now_iso,
        "totalRecipes": total_in_chunks,
        "recipeCount": len(recent_recipes_for_feed),
        "feedUrl": "https://raw.githubusercontent.com/Outrot174/KitchenMasterRecipes/main/data/recipes_feed.json",
        "sha256": feed_sha256,
        "chunks": chunks_meta
    }

    with open(MANIFEST_FILE, "w", encoding="utf-8") as f:
        json.dump(manifest, f, ensure_ascii=False, indent=2)

    print(f"[SUCCESS] Updated {MANIFEST_FILE}: Version {current_version}, {len(chunks_meta)} active chunks, {total_in_chunks} total recipes.")

# --- Функция повторной обработки существующих архивов (Re-processing) ---

def reprocess_existing_archives():
    """Повторно обрабатывает data/daily/*.json и data/recipes_feed.json для устранения пересорта и перевода."""
    print("[INFO] Re-processing existing archives to fix category misclassifications and residual English...")
    load_translation_cache()

    files_to_process = []
    if os.path.exists(DAILY_DIR):
        for fname in os.listdir(DAILY_DIR):
            if fname.endswith(".json"):
                files_to_process.append(os.path.join(DAILY_DIR, fname))
    if os.path.exists(FEED_FILE):
        files_to_process.append(FEED_FILE)

    modified_count = 0

    for fpath in files_to_process:
        try:
            with open(fpath, "r", encoding="utf-8") as f:
                recipes = json.load(f)
            updated_recipes = []
            file_changed = False

            for r in recipes:
                old_cat = r.get("category", "")
                title_orig = r.get("titleOrig", r.get("titleRu", ""))
                desc_orig = r.get("descriptionOrig", r.get("descriptionRu", ""))

                # 1. Строгая классификация
                new_cat = classify_strict_category(r.get("titleRu", ""), r.get("descriptionRu", ""), old_cat)
                if new_cat != old_cat:
                    r["category"] = new_cat
                    file_changed = True

                # 2. Перевод рецептов с остаточной латиницей или марstrings
                if r.get("sourceLang") == "EN":
                    steps_ru = r.get("stepsRu", [])
                    steps_orig = r.get("stepsOrig", [])
                    title_ru = r.get("titleRu", "")

                    first_step = steps_ru[0] if steps_ru else ""
                    latin_in_step = sum(1 for c in first_step if 'a' <= c <= 'z' or 'A' <= c <= 'Z')
                    cyrillic_in_step = sum(1 for c in first_step if 'а' <= c <= 'я' or 'А' <= c <= 'Я')

                    needs_trans = (
                        "(Шеф-рецепт)" in title_ru or
                        title_ru.strip().lower() == title_orig.strip().lower() or
                        steps_ru == steps_orig or
                        (latin_in_step > cyrillic_in_step and latin_in_step > 5)
                    )

                    if needs_trans:
                        t_ru, d_ru, ings_ru, norm_ings, st_ru = translate_english_recipe(
                            title_orig, desc_orig, r.get("ingredientsOrig", []), steps_orig
                        )
                        r["titleRu"] = t_ru
                        r["descriptionRu"] = d_ru
                        r["ingredientsRu"] = ings_ru
                        r["normalizedIngredients"] = sorted(list(norm_ings))
                        r["stepsRu"] = st_ru
                        file_changed = True
                        modified_count += 1

                # 3. Локализация мер в существующих ингредиентах
                for ing in r.get("ingredientsRu", []):
                    old_amt = ing.get("amount", "")
                    new_amt = localize_units(old_amt)
                    if new_amt != old_amt:
                        ing["amount"] = new_amt
                        file_changed = True

                updated_recipes.append(r)

            if file_changed:
                with open(fpath, "w", encoding="utf-8") as f:
                    json.dump(updated_recipes, f, ensure_ascii=False, indent=2)
                print(f"[SUCCESS] Re-processed and updated {fpath}")

        except Exception as e:
            print(f"[WARN] Error re-processing {fpath}: {e}")

    save_translation_cache()
    retained_chunks = prune_old_daily_chunks(MAX_DAILY_CHUNKS_KEEP)
    update_manifest_and_feed(retained_chunks)
    print(f"[SUCCESS] Re-processing complete. Fixed/updated {modified_count} English recipes.")

# --- Основной запуск ---

def main():
    os.makedirs(DATA_DIR, exist_ok=True)
    os.makedirs(DAILY_DIR, exist_ok=True)
    load_translation_cache()

    today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    print(f"=== KITCHENMASTER DAILY RECIPE PIPELINE: {today_str} ===")

    # 1. Загружаем индекс ранее собранных рецептов
    seen_ids = load_seen_ids()

    # 2. Выбираем буквы алфавита для TheMealDB
    hour = datetime.now(timezone.utc).hour
    alphabet = "abcdefghijklmnopqrstuvwxyz"
    shift = (hour // 6) * 6
    selected_letters = list(alphabet[shift % 26 : (shift + 7) % 26] or alphabet[:7])
    print(f"[INFO] Active letter sweep for this run: {selected_letters}")

    # 3. Сбор рецептов
    en_recipes = fetch_themealdb_batch(selected_letters, seen_ids, TARGET_EN_PER_RUN)
    ru_eda = fetch_russian_recipes_eda(seen_ids, TARGET_RU_PER_RUN // 2)
    remaining_ru = TARGET_RU_PER_RUN - len(ru_eda)
    ru_povarenok = fetch_russian_recipes_povarenok(seen_ids, remaining_ru)
    ru_recipes = ru_eda + ru_povarenok

    new_recipes = en_recipes + ru_recipes
    print(f"[SUMMARY] Total newly harvested recipes: {len(new_recipes)} (RU: {len(ru_recipes)}, EN: {len(en_recipes)})")

    # 4. Сохраняем в сегодняшний чанк
    save_daily_chunk(new_recipes, today_str)

    # 5. Ротация архивов (14 дней)
    retained_chunks = prune_old_daily_chunks(MAX_DAILY_CHUNKS_KEEP)

    # 6. Обновление манифеста и фида
    update_manifest_and_feed(retained_chunks)

    # 7. Сохранение индексов
    save_seen_ids(seen_ids)
    save_translation_cache()

    # 8. Повторная обработка существующих файлов для устранения пересорта
    reprocess_existing_archives()

    print("=== PIPELINE RUN COMPLETE ===")

if __name__ == "__main__":
    main()
