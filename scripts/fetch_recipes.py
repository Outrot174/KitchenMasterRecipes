#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
KitchenMaster High-Throughput Recipe Pipeline & World Cuisines Expansion
Скрипт высокоскоростного сбора рецептов со всего мира:
- Все кухни TheMealDB (Chinese, Japanese, Thai, Vietnamese, Indian, Italian, French, Mexican, etc.)
- Корейская кухня (koreanbapsang.com, mykoreankitchen.com)
- Китайская и азиатская кухня (thewoksoflife.com)
- Кавказская и грузинская кухня (1000.menu, povarenok.ru)
- Русские домашние порталы (povarenok.ru, eda.ru)
Все рецепты размечаются тегами кухонь ([Китайская кухня], [Корейская кухня], [Грузинская кухня] и т.д.)
для мгновенного поиска в приложении KitchenMaster.
Сбор выполняется без медленных серверных вызовов перевода за < 35 секунд.
"""

import concurrent.futures
import hashlib
import html
import http.client
import json
import os
import re
import time
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from datetime import datetime, timezone

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(PROJECT_ROOT, "data")
DAILY_DIR = os.path.join(DATA_DIR, "daily")
SEED_FILE = os.path.join(PROJECT_ROOT, "app", "src", "main", "assets", "recipes_seed.json")
FEED_FILE = os.path.join(DATA_DIR, "recipes_feed.json")
MANIFEST_FILE = os.path.join(DATA_DIR, "manifest.json")
SEEN_IDS_FILE = os.path.join(DATA_DIR, "seen_ids.json")
CRAWLER_STATE_FILE = os.path.join(DATA_DIR, "crawler_state.json")

TARGET_PER_RUN = 1200
MAX_DAILY_CHUNKS_KEEP = 14
FEED_CAP_RECIPES = 600
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

# --- Карта кухонь мира на русском языке для поиска ---
CUISINE_MAP = {
    "Chinese": ("Китайская кухня", "Chinese Cuisine"),
    "Japanese": ("Японская кухня", "Japanese Cuisine"),
    "Korean": ("Корейская кухня", "Korean Cuisine"),
    "South Korean": ("Корейская кухня", "Korean Cuisine"),
    "Thai": ("Тайская кухня", "Thai Cuisine"),
    "Vietnamese": ("Вьетнамская кухня", "Vietnamese Cuisine"),
    "Indian": ("Индийская кухня", "Indian Cuisine"),
    "Italian": ("Итальянская кухня", "Italian Cuisine"),
    "French": ("Французская кухня", "French Cuisine"),
    "Mexican": ("Мексиканская кухня", "Mexican Cuisine"),
    "Spanish": ("Испанская кухня", "Spanish Cuisine"),
    "Greek": ("Греческая кухня", "Greek Cuisine"),
    "Turkish": ("Турецкая кухня", "Turkish Cuisine"),
    "Georgian": ("Грузинская кухня", "Georgian Cuisine"),
    "Caucasian": ("Кавказская кухня", "Caucasian Cuisine"),
    "Moroccan": ("Марокканская кухня", "Moroccan Cuisine"),
    "British": ("Британская кухня", "British Cuisine"),
    "American": ("Американская кухня", "American Cuisine"),
    "Canadian": ("Канадская кухня", "Canadian Cuisine"),
    "Polish": ("Польская кухня", "Polish Cuisine"),
    "Portuguese": ("Португальская кухня", "Portuguese Cuisine"),
    "Jamaican": ("Ямайская кухня", "Jamaican Cuisine"),
    "Egyptian": ("Египетская кухня", "Egyptian Cuisine"),
    "Tunisian": ("Тунисская кухня", "Tunisian Cuisine"),
    "Croatian": ("Хорватская кухня", "Croatian Cuisine"),
    "Irish": ("Ирландская кухня", "Irish Cuisine"),
    "Dutch": ("Голландская кухня", "Dutch Cuisine"),
    "Malaysian": ("Малазийская кухня", "Malaysian Cuisine"),
    "Filipino": ("Филиппинская кухня", "Filipino Cuisine"),
    "Kenyan": ("Кенийская кухня", "Kenyan Cuisine"),
    "Ukrainian": ("Украинская кухня", "Ukrainian Cuisine"),
    "Russian": ("Русская кухня", "Russian Cuisine"),
    "Uzbek": ("Узбекская кухня", "Uzbek Cuisine"),
    "Armenian": ("Армянская кухня", "Armenian Cuisine"),
    "Azerbaijani": ("Азербайджанская кухня", "Azerbaijani Cuisine")
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
        (r"(?i)\bslices?\b", "ломт."),
        (r"(?i)\bstrips?\b", "полоски"),
        (r"(?i)\bgrams?\b|\bg\b", "г"),
        (r"(?i)\bml\b", "мл"),
        (r"(?i)\b(\d+[\d\s.,/-]*)\s*(?:liters?|litres?|l)\b", r"\1 л"),
        (r"(?i)\bpieces?\b", "шт."),
        (r"(?i)\bmedium\b", "средн."),
        (r"(?i)\blarge\b", "крупн."),
        (r"(?i)\bsmall\b", "мелк."),
        (r"(?i)\bhandful\b", "горсть"),
        (r"(?i)\bcans?\b", "банка"),
        (r"(?i)\b(?:packages?|pkg)\b", "упак."),
        (r"\b1 ст\.\b", "1 ст. л.")
    ]
    for pattern, repl in replacements:
        s = re.sub(pattern, repl, s)
    return s.strip()

def split_english_ingredient(full_str: str) -> tuple:
    s = full_str.strip()
    match = re.search(r"^(\d+[\d\s.,/-]*\s*(?:cups?|tablespoons?|tbsp?|tbs|teaspoons?|tsp|pinch(?:es)?|cloves?|ounces?|oz|pounds?|lbs?|grams?|g|ml|l|slices?|strips?|cans?|pieces?|packages?|pkg|handful)?)\s+(.*)$", s, re.IGNORECASE)
    if match and match.group(1).strip():
        amt = localize_units(match.group(1).strip())
        name = match.group(2).strip()
        return name if name else s, amt
    return s, "по вкусу"

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

def detect_russian_cuisine_tag(title: str, desc: str) -> str:
    combined = f"{title} {desc}".lower()
    if any(k in combined for k in ["грузинск", "харчо", "хачапури", "сациви", "хинкали", "ткемали", "лобио", "чахохбили", "кучмач", "аджарск", "тбилиси", "чашушули", "оджахури", "сулугуни", "чихиртма"]):
        return "Грузинская кухня"
    if any(k in combined for k in ["кавказск", "шашлык", "люля-кебаб", "долма", "чурчхела", "айран", "осетинск"]):
        return "Кавказская кухня"
    if any(k in combined for k in ["узбекск", "плов", "лагман", "самса", "манты", "шурпа"]):
        return "Узбекская кухня"
    if any(k in combined for k in ["корейск", "кимчи", "кимчхи", "пибимпаб", "кукси", "морковь по-корейски"]):
        return "Корейская кухня"
    if any(k in combined for k in ["китайск", "вок", "димсам", "кисло-сладк"]):
        return "Китайская кухня"
    if any(k in combined for k in ["японск", "суши", "рамен", "мисо", "роллы", "терияки"]):
        return "Японская кухня"
    if any(k in combined for k in ["итальянск", "пицца", "паста", "лазанья", "ризотто", "карбонара", "болоньезе"]):
        return "Итальянская кухня"
    if any(k in combined for k in ["французск", "жульен", "рататуй", "круассан", "киш"]):
        return "Французская кухня"
    if any(k in combined for k in ["тайск", "том ям", "том кха", "пад тай"]):
        return "Тайская кухня"
    if any(k in combined for k in ["мексиканск", "тако", "буррито", "фахитос", "кесадилья", "гуакамоле"]):
        return "Мексиканская кухня"
    if any(k in combined for k in ["украинск", "борщ с пампушками", "вареники", "деруны"]):
        return "Украинская кухня"
    if any(k in combined for k in ["русск", "щи", "уха", "окрошка", "солянка", "блины", "оладьи", "сырники"]):
        return "Русская кухня"
    return "Домашняя кухня"

def classify_strict_category(title: str, desc: str = "", orig_category: str = "") -> str:
    combined = f"{title} {desc}".lower()
    cat_lower = (orig_category or "").lower()

    first_keywords = [
        "soup", "stew", "chowder", "broth", "ramen", "pho", "borscht",
        "суп", "борщ", "щи", "солянка", "уха", "окрошка", "рассольник", "бульон", "гаспачо", "харчо", "шурпа"
    ]
    for k in first_keywords:
        if (len(k) <= 4 and re.search(rf"\b{k}\b", combined)) or (len(k) > 4 and k in combined):
            return "FIRST"

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

    salad_keywords = [
        "salad", "salads", "slaw", "coleslaw", "tabbouleh", "ceviche",
        "салат", "винегрет", "оливье", "цезарь", "греческий"
    ]
    for k in salad_keywords:
        if (len(k) <= 4 and re.search(rf"\b{k}\b", combined)) or (len(k) > 4 and k in combined):
            return "SALAD"

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
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.read().decode(encoding, errors="ignore")
    except http.client.IncompleteRead as e:
        return e.partial.decode(encoding, errors="ignore")

# --- Сбор TheMealDB по кухням мира (World Cuisines Multi-Area Sweep) ---

def parse_meal(meal: dict) -> dict:
    meal_id = f"mealdb-{meal.get('idMeal')}"
    title_orig = meal.get("strMeal", "").strip()
    category_raw = meal.get("strCategory", "Miscellaneous")
    area = meal.get("strArea", "International").strip()
    instructions = meal.get("strInstructions", "").strip()
    image_url = meal.get("strMealThumb", "")

    cuisine_ru, cuisine_en = CUISINE_MAP.get(area, (f"{area} кухня", area))

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

    desc_orig = f"[{cuisine_en} / {cuisine_ru}] Traditional {category_raw} dish: {title_orig}."
    desc_ru = f"[{cuisine_ru}] Традиционное блюдо: {title_orig}."
    category = classify_strict_category(title_orig, desc_orig, category_raw)

    ingredients_ru = [
        {"name": ing["name"], "amount": localize_units(ing["amount"])}
        for ing in ingredients_orig
    ]

    return build_recipe_dict(
        recipe_id=meal_id,
        title_ru=title_orig,
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
        normalized_ingredients=set(),
        steps_orig=steps_orig,
        steps_ru=steps_orig
    )

def fetch_themealdb_world_cuisines(seen_ids: set, target_count: int) -> list:
    print(f"[INFO] Sweeping TheMealDB for World Cuisines (Target: {target_count})...")
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

    print(f"[SUCCESS] Collected {len(recipes)} recipes from TheMealDB.")
    return recipes

# --- Сбор Корейской кухни (KoreanBapsang & MyKoreanKitchen) ---

def parse_schema_recipe_page(html_text: str, rid: str, default_cuisine: str = "Корейская кухня") -> dict:
    for m in re.finditer(r'<script[^>]*type=[\"\x27]application/ld\+json[\"\x27][^>]*>(.*?)</script>', html_text, re.DOTALL):
        try:
            data = json.loads(m.group(1))
            graph = data.get("@graph", [data]) if isinstance(data, dict) else (data if isinstance(data, list) else [data])
            for item in graph:
                if isinstance(item, dict) and item.get("@type") == "Recipe":
                    title = item.get("name")
                    if not title:
                        continue
                    clean_title = html.unescape(title).strip()
                    desc = html.unescape(item.get("description", "")).strip() or f"Authentic {default_cuisine} recipe: {clean_title}."
                    desc_tagged = f"[{default_cuisine}] {desc}"

                    cook_time = parse_iso_duration(item.get("cookTime") or item.get("totalTime") or "")
                    category = classify_strict_category(clean_title, desc)

                    raw_ings = item.get("recipeIngredient", [])
                    ingredients_orig = []
                    ingredients_ru = []
                    for r_ing in raw_ings:
                        clean_ing = html.unescape(str(r_ing)).strip()
                        name, amt = split_english_ingredient(clean_ing)
                        ingredients_orig.append({"name": name, "amount": amt})
                        ingredients_ru.append({"name": name, "amount": amt})

                    instructions = item.get("recipeInstructions", [])
                    steps = []
                    if isinstance(instructions, list):
                        for s in instructions:
                            if isinstance(s, dict):
                                t = s.get("text", "")
                            else:
                                t = str(s)
                            clean_t = html.unescape(re.sub(r"<[^>]+>", " ", t)).strip()
                            if len(clean_t) > 8:
                                steps.append(clean_t)
                    elif isinstance(instructions, str):
                        steps = [html.unescape(re.sub(r"<[^>]+>", " ", instructions)).strip()]

                    if not steps:
                        steps = [f"Follow traditional {default_cuisine} cooking techniques to prepare {clean_title}."]

                    img = item.get("image")
                    img_url = img[0] if isinstance(img, list) and img else (img if isinstance(img, str) else "")

                    return build_recipe_dict(
                        recipe_id=rid,
                        title_ru=clean_title,
                        title_orig=clean_title,
                        desc_ru=desc_tagged,
                        desc_orig=desc_tagged,
                        category=category,
                        cook_time=cook_time,
                        servings=4,
                        image_url=img_url,
                        source_lang="EN",
                        ingredients_orig=ingredients_orig,
                        ingredients_ru=ingredients_ru,
                        normalized_ingredients=set(),
                        steps_orig=steps,
                        steps_ru=steps
                    )
        except Exception:
            pass
    return None

def fetch_korean_recipes(seen_ids: set, target_count: int, cursor: int = 0) -> tuple:
    print(f"[INFO] Fetching authentic Korean recipes (Target: {target_count}, cursor: {cursor})...")
    recipes = []
    feed_urls = [
        ("https://www.koreanbapsang.com/feed/", "Корейская кухня"),
        ("https://mykoreankitchen.com/feed/", "Корейская кухня")
    ]
    links = []
    for f_url, cuisine_name in feed_urls:
        try:
            req = urllib.request.Request(f_url, headers=HTTP_HEADERS)
            with urllib.request.urlopen(req, timeout=10) as r:
                root = ET.fromstring(r.read())
                for item in root.findall(".//item"):
                    l = item.find("link")
                    if l is not None and l.text:
                        links.append((l.text.strip(), cuisine_name))
        except Exception as e:
            print(f"[WARN] Error fetching Korean RSS {f_url}: {e}")

    # Срез по курсору
    start_idx = cursor % max(1, len(links)) if links else 0
    candidate_links = links[start_idx : start_idx + target_count * 2]

    def fetch_single(item):
        link, cuisine = item
        clean_id = f"korean-{abs(hash(link)) % 1000000}"
        if clean_id in seen_ids:
            return None
        try:
            p_html = http_get_sync(link, timeout=10)
            return parse_schema_recipe_page(p_html, clean_id, cuisine)
        except Exception:
            return None

    with concurrent.futures.ThreadPoolExecutor(max_workers=CONCURRENCY_LIMIT) as executor:
        for r in executor.map(fetch_single, candidate_links):
            if r and r["id"] not in seen_ids:
                seen_ids.add(r["id"])
                recipes.append(r)
                if len(recipes) >= target_count:
                    break

    next_cursor = start_idx + len(candidate_links)
    print(f"[SUCCESS] Collected {len(recipes)} authentic Korean recipes.")
    return recipes, next_cursor

# --- Сбор Китайской / Азиатской кухни (TheWoksOfLife) ---

def fetch_asian_chinese_recipes(seen_ids: set, target_count: int, cursor: int = 0) -> tuple:
    print(f"[INFO] Fetching authentic Chinese & Asian recipes (Target: {target_count}, cursor: {cursor})...")
    recipes = []
    feed_urls = [
        ("https://thewoksoflife.com/feed/", "Китайская кухня"),
        ("https://omnivorescookbook.com/feed/", "Китайская кухня")
    ]
    links = []
    for f_url, cuisine_name in feed_urls:
        try:
            req = urllib.request.Request(f_url, headers=HTTP_HEADERS)
            with urllib.request.urlopen(req, timeout=10) as r:
                root = ET.fromstring(r.read())
                for item in root.findall(".//item"):
                    l = item.find("link")
                    if l is not None and l.text:
                        links.append((l.text.strip(), cuisine_name))
        except Exception as e:
            print(f"[WARN] Error fetching Asian RSS {f_url}: {e}")

    start_idx = cursor % max(1, len(links)) if links else 0
    candidate_links = links[start_idx : start_idx + target_count * 2]

    def fetch_single(item):
        link, cuisine = item
        clean_id = f"asian-{abs(hash(link)) % 1000000}"
        if clean_id in seen_ids:
            return None
        try:
            p_html = http_get_sync(link, timeout=10)
            return parse_schema_recipe_page(p_html, clean_id, cuisine)
        except Exception:
            return None

    with concurrent.futures.ThreadPoolExecutor(max_workers=CONCURRENCY_LIMIT) as executor:
        for r in executor.map(fetch_single, candidate_links):
            if r and r["id"] not in seen_ids:
                seen_ids.add(r["id"])
                recipes.append(r)
                if len(recipes) >= target_count:
                    break

    next_cursor = start_idx + len(candidate_links)
    print(f"[SUCCESS] Collected {len(recipes)} authentic Chinese/Asian recipes.")
    return recipes, next_cursor

# --- Сбор Кавказской и Грузинской кухни (1000.menu) ---

def parse_menu1000_page(html_text: str, rid: str, url: str, default_cuisine: str = "Грузинская кухня") -> dict:
    title_m = re.search(r"<h1[^>]*>(.*?)</h1>", html_text, re.DOTALL)
    if not title_m:
        return None
    title = html.unescape(title_m.group(1)).strip()
    title = re.sub(r"<[^>]+>", " ", title).strip()

    desc_m = re.search(r"<meta itemprop=[\"\x27]description[\"\x27] content=[\"\x27](.*?)[\"\x27]", html_text)
    raw_desc = html.unescape(desc_m.group(1)).strip() if desc_m else f"Традиционный рецепт: {title}."
    detected = detect_russian_cuisine_tag(title, raw_desc)
    cuisine_tag = detected if detected != "Домашняя кухня" else default_cuisine
    desc = f"[{cuisine_tag}] {raw_desc}"

    img_m = re.search(r"property=[\"\x27]og:image[\"\x27][^>]*content=[\"\x27](.*?)[\"\x27]", html_text)
    img_url = img_m.group(1) if img_m else ""

    time_m = re.search(r"itemprop=[\"\x27](?:cookTime|totalTime)[\"\x27][^>]*content=[\"\x27](.*?)[\"\x27]", html_text)
    cook_time = parse_iso_duration(time_m.group(1) if time_m else "")

    servings_m = re.search(r"itemprop=[\"\x27]recipeYield[\"\x27][^>]*content=[\"\x27](\d+)[\"\x27]", html_text)
    servings = int(servings_m.group(1)) if servings_m else 4

    category = classify_strict_category(title, desc)

    raw_ings = re.findall(r"<meta itemprop=[\"\x27]recipeIngredient[\"\x27] content=[\"\x27](.*?)[\"\x27]", html_text)
    if not raw_ings:
        return None

    ingredients_orig = []
    ingredients_ru = []
    normalized_ingredients = set()

    for r_ing in raw_ings:
        clean_ing = " ".join(html.unescape(r_ing).split())
        name, amount = split_russian_ingredient(clean_ing)
        ingredients_orig.append({"name": name, "amount": amount})
        ingredients_ru.append({"name": name, "amount": amount})
        norm = normalize_ingredient(name)
        if norm and norm.lower() not in PANTRY_STAPLES and len(norm) > 2:
            normalized_ingredients.add(norm)

    steps = []
    divs = re.findall(r"<div class=[\"\x27]instruction[\"\x27][^>]*>(.*?)</div>", html_text, re.DOTALL)
    for div in divs:
        p_matches = re.findall(r"<p(?![^>]*class=[\"\x27]infoblock)(?:[^>]*)>(.*?)</p>", div, re.DOTALL)
        for p in p_matches:
            clean_p = " ".join(html.unescape(re.sub(r"<[^>]+>", " ", p)).split())
            if len(clean_p) > 10 and not clean_p.startswith("✅"):
                steps.append(clean_p)
                break
    if not steps:
        steps = [f"Приготовить блюдо «{title}» по рецепту и подать к столу."]

    return build_recipe_dict(
        recipe_id=rid,
        title_ru=title,
        title_orig=title,
        desc_ru=desc,
        desc_orig=desc,
        category=category,
        cook_time=cook_time,
        servings=servings,
        image_url=img_url,
        source_lang="RU",
        ingredients_orig=ingredients_orig,
        ingredients_ru=ingredients_ru,
        normalized_ingredients=normalized_ingredients,
        steps_orig=steps,
        steps_ru=steps
    )

def fetch_caucasian_georgian_recipes(seen_ids: set, target_count: int) -> list:
    print(f"[INFO] Fetching Caucasian & Georgian recipes from 1000.menu (Target: {target_count})...")
    recipes = []
    catalogs = [
        ("https://1000.menu/catalog/gruzinskaya-kuxnya", "Грузинская кухня"),
        ("https://1000.menu/catalog/kavkazskaya-kuxnya", "Кавказская кухня")
    ]
    links = []
    for cat_url, default_cuisine in catalogs:
        try:
            html_txt = http_get_sync(cat_url, timeout=10)
            matches = re.findall(r'href=[\"\x27](/cooking/\d+-[^\"\x27]+)[\"\x27]', html_txt)
            for m in matches:
                full_url = f"https://1000.menu{m}"
                if not any(l[0] == full_url for l in links):
                    links.append((full_url, default_cuisine))
        except Exception as e:
            print(f"[WARN] Error fetching catalog {cat_url}: {e}")

    def fetch_single(item):
        url, default_cuisine = item
        clean_id = f"menu1000-{abs(hash(url)) % 1000000}"
        if clean_id in seen_ids:
            return None
        try:
            html_txt = http_get_sync(url, timeout=10)
            return parse_menu1000_page(html_txt, clean_id, url, default_cuisine)
        except Exception:
            return None

    with concurrent.futures.ThreadPoolExecutor(max_workers=CONCURRENCY_LIMIT) as executor:
        for r in executor.map(fetch_single, links[:target_count * 2]):
            if r and r["id"] not in seen_ids:
                seen_ids.add(r["id"])
                recipes.append(r)
                if len(recipes) >= target_count:
                    break

    print(f"[SUCCESS] Collected {len(recipes)} Caucasian & Georgian recipes from 1000.menu.")
    return recipes

# --- Сбор русских домашних рецептов (Povarenok.ru) ---

def parse_povarenok_page(html_text: str, rid: str, link: str) -> dict:
    name_m = re.search(r"<h1[^>]*itemprop=[\"\x27]name[\"\x27][^>]*>(.*?)</h1>", html_text, re.DOTALL)
    if not name_m:
        return None
    title = html.unescape(name_m.group(1).strip())

    desc_m = re.search(r"<div[^>]*itemprop=[\"\x27]description[\"\x27][^>]*>(.*?)</div>", html_text, re.DOTALL)
    raw_desc = html.unescape(re.sub(r"<[^>]+>", " ", desc_m.group(1)).strip()) if desc_m else f"Традиционный домашний рецепт: {title}."
    cuisine_tag = detect_russian_cuisine_tag(title, raw_desc)
    desc = f"[{cuisine_tag}] {raw_desc}"

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

# --- Хранилище состояния пагинации (crawler_state.json) ---

def load_crawler_state() -> dict:
    default_state = {
        "povarenok_page": 1,
        "eda_page": 1,
        "korean_cursor": 0,
        "asian_cursor": 0,
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

    current_version = 13
    if os.path.exists(MANIFEST_FILE):
        try:
            with open(MANIFEST_FILE, "r", encoding="utf-8") as f:
                prev_manifest = json.load(f)
                current_version = prev_manifest.get("version", 12) + 1
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
    """Быстрая нормализация существующих архивов: добавление тегов кухонь и локализация мер."""
    print("[INFO] Re-processing existing archives to ensure world cuisine tags and localized units...")
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
                title_orig = r.get("titleOrig", r.get("titleRu", ""))
                desc_orig = r.get("descriptionOrig", r.get("descriptionRu", ""))

                # Категория
                new_cat = classify_strict_category(r.get("titleRu", ""), r.get("descriptionRu", ""), old_cat)
                if new_cat != old_cat:
                    r["category"] = new_cat
                    file_changed = True

                # Теги кухонь для поиска
                desc_ru = r.get("descriptionRu", "")
                if not desc_ru.startswith("[") or "кухня]" not in desc_ru:
                    tag = detect_russian_cuisine_tag(r.get("titleRu", ""), desc_ru)
                    r["descriptionRu"] = f"[{tag}] {desc_ru}"
                    file_changed = True

                # Локализация мер
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
    print(f"=== KITCHENMASTER WORLD CUISINES PIPELINE: {today_str} ===")

    crawler_state = load_crawler_state()
    seen_ids = load_seen_ids()

    # 1. TheMealDB (World Cuisines Multi-Area Sweep)
    en_world = fetch_themealdb_world_cuisines(seen_ids, 400)

    # 2. Корейская кухня (KoreanBapsang & MyKoreanKitchen)
    k_cursor = crawler_state.get("korean_cursor", 0)
    en_korean, next_k_cursor = fetch_korean_recipes(seen_ids, 20, cursor=k_cursor)
    crawler_state["korean_cursor"] = next_k_cursor

    # 3. Китайская и Азиатская кухня (TheWoksOfLife & OmnivoresCookbook)
    a_cursor = crawler_state.get("asian_cursor", 0)
    en_asian, next_a_cursor = fetch_asian_chinese_recipes(seen_ids, 20, cursor=a_cursor)
    crawler_state["asian_cursor"] = next_a_cursor

    # 4. Кавказская и Грузинская кухня (1000.menu)
    ru_caucasian = fetch_caucasian_georgian_recipes(seen_ids, 30)

    # 5. Русские домашние рецепты с пагинацией (Povarenok.ru)
    p_start = crawler_state.get("povarenok_page", 1)
    ru_povarenok, next_p_page = fetch_russian_recipes_povarenok(seen_ids, 450, start_page=p_start, page_count=40)
    crawler_state["povarenok_page"] = next_p_page

    new_recipes = en_world + en_korean + en_asian + ru_caucasian + ru_povarenok
    print(f"[SUMMARY] Newly harvested: {len(new_recipes)} recipes across World Cuisines in {time.time() - t_start:.2f}s")

    # 6. Сохранение сегодняшнего чанка
    save_daily_chunk(new_recipes, today_str)

    # 7. Ротация архивов и обновление манифеста
    retained_chunks = prune_old_daily_chunks(MAX_DAILY_CHUNKS_KEEP)
    update_manifest_and_feed(retained_chunks)

    # 8. Сохранение индексов и состояния краулера
    save_seen_ids(seen_ids)
    save_crawler_state(crawler_state)
    reprocess_existing_archives()

    total_duration = time.time() - t_start
    print(f"=== WORLD CUISINES PIPELINE COMPLETE in {total_duration:.2f}s (Ingested: {len(new_recipes)} recipes) ===")

if __name__ == "__main__":
    main()
