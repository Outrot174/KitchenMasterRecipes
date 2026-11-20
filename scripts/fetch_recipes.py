#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
KitchenMaster High-Throughput Async Recipe Pipeline
Скрипт высокоскоростного сбора и нормализации кулинарных рецептов (1,000+ за запуск)
без медленных серверных вызовов Google Translate.
Для англоязычных рецептов сохраняются оригиналы, локализуются единицы измерения,
а полный перевод текста выполняется на клиенте по требованию (on-demand).
Поддерживает пагинацию через data/crawler_state.json, разбивку на ежедневные дельта-чанки
(data/daily/YYYY-MM-DD.json), манифест (data/manifest.json) и фид (data/recipes_feed.json).
"""

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

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(PROJECT_ROOT, "data")
DAILY_DIR = os.path.join(DATA_DIR, "daily")
SEED_FILE = os.path.join(PROJECT_ROOT, "app", "src", "main", "assets", "recipes_seed.json")
FEED_FILE = os.path.join(DATA_DIR, "recipes_feed.json")
MANIFEST_FILE = os.path.join(DATA_DIR, "manifest.json")
SEEN_IDS_FILE = os.path.join(DATA_DIR, "seen_ids.json")
CRAWLER_STATE_FILE = os.path.join(DATA_DIR, "crawler_state.json")

TARGET_RU_PER_RUN = 500
TARGET_EN_PER_RUN = 550
MAX_DAILY_CHUNKS_KEEP = 14
FEED_CAP_RECIPES = 500
CONCURRENCY_LIMIT = 25

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

def localize_units(amount: str) -> str:
    """Локализует кулинарные меры измерения на русский язык."""
    if not amount:
        return "по вкусу"
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

    # 4. САЛАТЫ (SALAD)
    salad_keywords = [
        "salad", "salads", "slaw", "coleslaw", "tabbouleh", "ceviche",
        "салат", "винегрет", "оливье", "цезарь", "греческий"
    ]
    for k in salad_keywords:
        if (len(k) <= 4 and re.search(rf"\b{k}\b", combined)) or (len(k) > 4 and k in combined):
            return "SALAD"

    # 5. ВТОРЫЕ БЛЮДА (SECOND)
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
    allergens = []
    text_check = f"{title_ru} {desc_ru} " + " ".join([i['name'] for i in ingredients_ru])
    text_check_lower = text_check.lower()

    if any(x in text_check_lower for x in ["молок", "сыр", "сливк", "творог", "масло сливоч", "milk", "cheese", "cream", "butter"]):
        allergens.append("Лактоза")
    if any(x in text_check_lower for x in ["мука", "пшениц", "макарон", "паста", "спагетти", "хлеб", "глютен", "flour", "wheat", "pasta", "bread"]):
        allergens.append("Глютен")
    if any(x in text_check_lower for x in ["яйц", "яичн", "желток", "egg"]):
        allergens.append("Яйца")
    if any(x in text_check_lower for x in ["орех", "миндал", "фундук", "арахис", "кешью", "nut", "peanut", "almond"]):
        allergens.append("Орехи")
    if any(x in text_check_lower for x in ["рыб", "лосос", "семг", "форел", "креветк", "тунец", "fish", "salmon", "shrimp"]):
        allergens.append("Морепродукты")

    return {
        "id": recipe_id,
        "titleRu": title_ru,
        "titleOrig": title_orig,
        "descriptionRu": desc_ru,
        "descriptionOrig": desc_orig,
        "category": category,
        "cookTimeMinutes": cook_time,
        "servings": max(1, servings),
        "imageUrl": image_url or "",
        "sourceLang": source_lang,
        "isFavorite": False,
        "ingredientsRu": deduplicate_ingredients(ingredients_ru),
        "ingredientsOrig": deduplicate_ingredients(ingredients_orig),
        "normalizedIngredients": sorted(list(normalized_ingredients)),
        "stepsRu": steps_ru,
        "stepsOrig": steps_orig,
        "allergens": allergens
    }

def http_get_sync(url: str, timeout: int = 10, encoding: str = "utf-8") -> str:
    req = urllib.request.Request(url, headers=HTTP_HEADERS)
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return resp.read().decode(encoding, errors="ignore")

# --- Сбор TheMealDB (Быстрый сбор оригиналов без серверного перевода) ---

def parse_meal(meal: dict) -> dict:
    meal_id = f"mealdb-{meal.get('idMeal')}"
    title_orig = meal.get("strMeal", "").strip()
    category_raw = meal.get("strCategory", "Miscellaneous")
    instructions = meal.get("strInstructions", "").strip()
    image_url = meal.get("strMealThumb", "")

    raw_steps = [s.strip() for s in re.split(r"\r?\n|\.\s+", instructions) if len(s.strip()) > 10]
    steps_orig = raw_steps[:10] if raw_steps else [instructions]

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
    category = classify_strict_category(title_orig, desc_orig, category_raw)

    # Без медленного перевода на сервере: сохраняем оригинал, локализуем единицы
    ingredients_ru = [
        {"name": ing["name"], "amount": localize_units(ing["amount"])}
        for ing in ingredients_orig
    ]

    return build_recipe_dict(
        recipe_id=meal_id,
        title_ru=title_orig,
        title_orig=title_orig,
        desc_ru=desc_orig,
        desc_orig=desc_orig,
        category=category,
        cook_time=35,
        servings=4,
        image_url=image_url,
        source_lang="EN",
        ingredients_orig=ingredients_orig,
        ingredients_ru=ingredients_ru,
        normalized_ingredients=set(),
        steps_orig=steps_orig,
        steps_ru=steps_orig
    )

def fetch_themealdb_concurrent(seen_ids: set, target_count: int) -> list:
    print(f"[INFO] Fetching international recipes from TheMealDB (concurrent letters, target: {target_count})...")
    recipes = []
    alphabet = list("abcdefghijklmnopqrstuvwxyz")

    def fetch_letter(l):
        url = f"https://www.themealdb.com/api/json/v1/1/search.php?f={l}"
        try:
            content = http_get_sync(url, timeout=10)
            data = json.loads(content)
            return data.get("meals") or []
        except Exception:
            return []

    with concurrent.futures.ThreadPoolExecutor(max_workers=12) as executor:
        for meals in executor.map(fetch_letter, alphabet):
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
            if len(recipes) >= target_count:
                break

    print(f"[SUCCESS] Collected {len(recipes)} international recipes from TheMealDB.")
    return recipes

# --- Сбор DummyJSON Recipes ---

def fetch_dummyjson_recipes(seen_ids: set, target_count: int) -> list:
    print(f"[INFO] Fetching recipes from DummyJSON (target: {target_count})...")
    recipes = []
    url = "https://dummyjson.com/recipes?limit=50"
    try:
        content = http_get_sync(url, timeout=10)
        data = json.loads(content)
        for item in data.get("recipes", []):
            rid = f"dummyjson-{item.get('id')}"
            if rid in seen_ids:
                continue
            title_orig = item.get("name", "").strip()
            instructions = item.get("instructions", [])
            steps_orig = instructions if isinstance(instructions, list) else [str(instructions)]
            ing_names = item.get("ingredients", [])
            ingredients_orig = [{"name": name, "amount": "по вкусу"} for name in ing_names]
            ingredients_ru = [{"name": name, "amount": "по вкусу"} for name in ing_names]
            cook_time = item.get("cookTimeMinutes", 30) + item.get("prepTimeMinutes", 10)
            servings = item.get("servings", 4)
            image_url = item.get("image", "")
            desc_orig = f"Delicious {title_orig} with {item.get('cuisine', 'international')} style."
            category = classify_strict_category(title_orig, desc_orig, item.get("mealType", [""])[0] if item.get("mealType") else "")

            parsed = build_recipe_dict(
                recipe_id=rid,
                title_ru=title_orig,
                title_orig=title_orig,
                desc_ru=desc_orig,
                desc_orig=desc_orig,
                category=category,
                cook_time=cook_time,
                servings=servings,
                image_url=image_url,
                source_lang="EN",
                ingredients_orig=ingredients_orig,
                ingredients_ru=ingredients_ru,
                normalized_ingredients=set(),
                steps_orig=steps_orig,
                steps_ru=steps_orig
            )
            recipes.append(parsed)
            seen_ids.add(rid)
            if len(recipes) >= target_count:
                break
    except Exception as e:
        print(f"[WARN] Error fetching DummyJSON: {e}")

    print(f"[SUCCESS] Collected {len(recipes)} recipes from DummyJSON.")
    return recipes

# --- Сбор русских рецептов (Povarenok.ru) ---

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

def fetch_russian_recipes_povarenok(seen_ids: set, target_count: int, start_page: int = 1, page_count: int = 40) -> tuple:
    print(f"[INFO] Fetching Russian recipes from povarenok.ru (pages {start_page}..{start_page + page_count - 1}, target: {target_count})...")
    recipes = []
    pages = range(start_page, start_page + page_count)

    def fetch_page_links(p):
        url = f"https://www.povarenok.ru/recipes/~{p}/" if p > 1 else "https://www.povarenok.ru/recipes/"
        try:
            p_html = http_get_sync(url, timeout=8, encoding="windows-1251")
            return set(re.findall(r"href=[\"\x27](https://www.povarenok.ru/recipes/show/(\d+)/)[\"\x27]", p_html))
        except Exception:
            return set()

    candidate_links = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        for links in executor.map(fetch_page_links, pages):
            for link, rid in links:
                recipe_id = f"povarenok-{rid}"
                if recipe_id not in seen_ids and (link, rid) not in candidate_links:
                    candidate_links.append((link, rid))

    print(f"[INFO] Found {len(candidate_links)} candidates from povarenok.ru")

    def fetch_single(item):
        link, rid = item
        try:
            html_text = http_get_sync(link, timeout=8, encoding="windows-1251")
            return parse_povarenok_page(html_text, rid, link)
        except Exception:
            return None

    with concurrent.futures.ThreadPoolExecutor(max_workers=CONCURRENCY_LIMIT) as executor:
        futures = {executor.submit(fetch_single, item): item for item in candidate_links}
        for future in concurrent.futures.as_completed(futures):
            parsed = future.result()
            if parsed and parsed["id"] not in seen_ids:
                seen_ids.add(parsed["id"])
                recipes.append(parsed)
                if len(recipes) >= target_count:
                    break

    next_page = start_page + page_count
    print(f"[SUCCESS] Collected {len(recipes)} unique Russian recipes from povarenok.ru.")
    return recipes, next_page

# --- Сбор русских рецептов (Eda.ru) ---

def fetch_russian_recipes_eda(seen_ids: set, target_count: int, start_page: int = 1, page_count: int = 20) -> tuple:
    print(f"[INFO] Fetching Russian recipes from eda.ru (pages {start_page}..{start_page + page_count - 1}, target: {target_count})...")
    recipes = []
    pages = range(start_page, start_page + page_count)

    def fetch_eda_page_recipes(p):
        url = f"https://eda.ru/recepty?page={p}"
        page_recipes = []
        try:
            p_html = http_get_sync(url, timeout=8)
            scripts = re.findall(r'<script[^>]*type=[\"\x27]application/ld\+json[\"\x27][^>]*>(.*?)</script>', p_html, re.DOTALL)
            for s in scripts:
                try:
                    data = json.loads(s.strip())
                    graph = data.get("@graph", []) if isinstance(data, dict) else (data if isinstance(data, list) else [data])
                    for item in graph:
                        if isinstance(item, dict) and item.get("@type") == "Recipe":
                            name = item.get("name")
                            if not name:
                                continue
                            clean_name = html.unescape(name).strip()
                            clean_id = f"eda-{abs(hash(clean_name)) % 1000000}"
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
                                    text = st.get("text", "") if isinstance(st, dict) else str(st)
                                    text_clean = html.unescape(re.sub(r"<[^>]+>", " ", text)).strip()
                                    if len(text_clean) > 8:
                                        steps.append(text_clean)
                            elif isinstance(steps_data, str):
                                steps = [html.unescape(re.sub(r"<[^>]+>", " ", steps_data)).strip()]
                            if not steps:
                                steps = [f"Приготовить «{clean_name}» согласно рецептуре и подать к столу."]

                            img = item.get("image")
                            img_url = img[0] if isinstance(img, list) and img else (img if isinstance(img, str) else "")

                            page_recipes.append(build_recipe_dict(
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
                            ))
                except Exception:
                    pass
        except Exception:
            pass
        return page_recipes

    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        for prs in executor.map(fetch_eda_page_recipes, pages):
            for r in prs:
                if r["id"] not in seen_ids:
                    seen_ids.add(r["id"])
                    recipes.append(r)
                    if len(recipes) >= target_count:
                        break
            if len(recipes) >= target_count:
                break

    next_page = start_page + page_count
    print(f"[SUCCESS] Collected {len(recipes)} Russian recipes from eda.ru.")
    return recipes, next_page

# --- Хранилище состояния пагинации (crawler_state.json) ---

def load_crawler_state() -> dict:
    default_state = {
        "povarenok_page": 1,
        "eda_page": 1,
        "last_run": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    }
    if os.path.exists(CRAWLER_STATE_FILE):
        try:
            with open(CRAWLER_STATE_FILE, "r", encoding="utf-8") as f:
                state = json.load(f)
                for k, v in default_state.items():
                    if k not in state:
                        state[k] = v
                return state
        except Exception:
            pass
    return default_state

def save_crawler_state(state: dict):
    state["last_run"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    with open(CRAWLER_STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)
    print(f"[INFO] Saved crawler state: {state}")

# --- Индекс виденных ID и управление чанками ---

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

    current_version = 12
    if os.path.exists(MANIFEST_FILE):
        try:
            with open(MANIFEST_FILE, "r", encoding="utf-8") as f:
                prev_manifest = json.load(f)
                current_version = prev_manifest.get("version", 11) + 1
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

def reprocess_existing_archives():
    """Быстрая нормализация существующих архивов без HTTP запросов перевода."""
    print("[INFO] Re-processing existing archives for category classification and unit localization...")
    files_to_process = []
    if os.path.exists(DAILY_DIR):
        for fname in os.listdir(DAILY_DIR):
            if fname.endswith(".json"):
                files_to_process.append(os.path.join(DAILY_DIR, fname))
    if os.path.exists(FEED_FILE):
        files_to_process.append(FEED_FILE)

    for fpath in files_to_process:
        try:
            with open(fpath, "r", encoding="utf-8") as f:
                recipes = json.load(f)
            updated_recipes = []
            file_changed = False

            for r in recipes:
                old_cat = r.get("category", "")
                new_cat = classify_strict_category(r.get("titleRu", ""), r.get("descriptionRu", ""), old_cat)
                if new_cat != old_cat:
                    r["category"] = new_cat
                    file_changed = True

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
        except Exception as e:
            print(f"[WARN] Error re-processing {fpath}: {e}")

    retained_chunks = prune_old_daily_chunks(MAX_DAILY_CHUNKS_KEEP)
    update_manifest_and_feed(retained_chunks)

def main():
    t_start = time.time()
    os.makedirs(DATA_DIR, exist_ok=True)
    os.makedirs(DAILY_DIR, exist_ok=True)

    today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    print(f"=== KITCHENMASTER HIGH-SPEED RECIPE PIPELINE: {today_str} ===")

    crawler_state = load_crawler_state()
    seen_ids = load_seen_ids()

    # 1. Сбор международных рецептов (TheMealDB + DummyJSON)
    en_mealdb = fetch_themealdb_concurrent(seen_ids, TARGET_EN_PER_RUN - 50)
    en_dummy = fetch_dummyjson_recipes(seen_ids, 50)
    en_recipes = en_mealdb + en_dummy

    # 2. Сбор русских рецептов с пагинацией
    p_start = crawler_state.get("povarenok_page", 1)
    ru_povarenok, next_p_page = fetch_russian_recipes_povarenok(seen_ids, TARGET_RU_PER_RUN, start_page=p_start, page_count=40)
    crawler_state["povarenok_page"] = next_p_page

    eda_start = crawler_state.get("eda_page", 1)
    ru_eda, next_eda_page = fetch_russian_recipes_eda(seen_ids, 100, start_page=eda_start, page_count=20)
    crawler_state["eda_page"] = next_eda_page

    ru_recipes = ru_povarenok + ru_eda
    new_recipes = en_recipes + ru_recipes

    print(f"[SUMMARY] Total newly harvested recipes: {len(new_recipes)} (RU: {len(ru_recipes)}, EN: {len(en_recipes)}) in {time.time() - t_start:.2f}s")

    # 3. Сохранение сегодняшнего чанка
    save_daily_chunk(new_recipes, today_str)

    # 4. Ротация архивов и обновление манифеста
    retained_chunks = prune_old_daily_chunks(MAX_DAILY_CHUNKS_KEEP)
    update_manifest_and_feed(retained_chunks)

    # 5. Сохранение индексов и состояния
    save_seen_ids(seen_ids)
    save_crawler_state(crawler_state)
    reprocess_existing_archives()

    total_duration = time.time() - t_start
    print(f"=== PIPELINE RUN COMPLETE in {total_duration:.2f}s (Ingested: {len(new_recipes)} recipes) ===")

if __name__ == "__main__":
    main()
