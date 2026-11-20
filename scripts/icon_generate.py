#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
KitchenMaster Image-based PNG Icon Generator
Использует мастер-изображение icon_source.jpg для создания полного комплекта
растровых PNG иконок для всех плотностей и форматов Android:
- mipmap-mdpi, mipmap-hdpi, mipmap-xhdpi, mipmap-xxhdpi, mipmap-xxxhdpi
- drawable-nodpi, drawable
- ic_launcher.png (squircle)
- ic_launcher_round.png (round)
- ic_launcher_foreground.png (adaptive foreground)
- icon.png (512x512 мастер-растр в корне)
"""

import os
import sys
from PIL import Image, ImageDraw

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__))) if os.path.basename(os.path.dirname(os.path.abspath(__file__))) == "scripts" else os.path.dirname(os.path.abspath(__file__))

DENSITIES = {
    "mipmap-mdpi": {"icon": (48, 48), "fg": (108, 108)},
    "mipmap-hdpi": {"icon": (72, 72), "fg": (162, 162)},
    "mipmap-xhdpi": {"icon": (96, 96), "fg": (216, 216)},
    "mipmap-xxhdpi": {"icon": (144, 144), "fg": (324, 324)},
    "mipmap-xxxhdpi": {"icon": (192, 192), "fg": (432, 432)}
}

def find_source_image() -> str:
    candidates = [
        os.path.join(PROJECT_ROOT, "icon_source.jpg"),
        os.path.join(PROJECT_ROOT, "icon_source.png"),
        "/data/user/0/com.agy/files/.gemini/antigravity-app/brain/62ac1bcd-0fb0-40c9-8ddd-b0400cd34253/.user_uploaded/media_1795168617180_12a178ad.jpg"
    ]
    for c in candidates:
        if os.path.exists(c):
            return c
    raise FileNotFoundError("Source icon image not found in candidates")

def main():
    print("=== KITCHENMASTER IMAGE-BASED ICON GENERATOR ===")
    src_file = find_source_image()
    print(f"Source Image: {src_file}")
    print(f"Target Project Root: {PROJECT_ROOT}")

    # Загружаем мастер-изображение
    img = Image.open(src_file).convert("RGBA")
    
    # Обрезаем белые поля вокруг скругленного квадрата (область 30,30 -> 994,994)
    crop = img.crop((30, 30, 994, 994))
    w, h = crop.size

    # 1. Мастер Squircle (скругленный квадрат для обычных лаунчеров)
    sq_mask = Image.new("L", (w, h), 0)
    ImageDraw.Draw(sq_mask).rounded_rectangle((0, 0, w, h), radius=210, fill=255)
    master_squircle = crop.copy()
    master_squircle.putalpha(sq_mask)

    # 2. Мастер Round (круг для круглых лаунчеров)
    circ_mask = Image.new("L", (w, h), 0)
    ImageDraw.Draw(circ_mask).ellipse((8, 8, w - 8, h - 8), fill=255)
    master_round = crop.copy()
    master_round.putalpha(circ_mask)

    # 3. Мастер Foreground (для адаптивных иконок 108dp)
    # Полотно 1024x1024 с фоном цвета темной темы (#211B1A = 33, 27, 26)
    master_fg = Image.new("RGBA", (1024, 1024), (33, 27, 26, 255))
    scale_size = 880
    scaled_crop = crop.resize((scale_size, scale_size), Image.Resampling.LANCZOS)
    offset = (1024 - scale_size) // 2
    master_fg.paste(scaled_crop, (offset, offset))

    res_dir = os.path.join(PROJECT_ROOT, "app", "src", "main", "res")
    drawable_dir = os.path.join(res_dir, "drawable")
    drawable_nodpi_dir = os.path.join(res_dir, "drawable-nodpi")
    mipmap_v26_dir = os.path.join(res_dir, "mipmap-anydpi-v26")

    os.makedirs(drawable_dir, exist_ok=True)
    os.makedirs(drawable_nodpi_dir, exist_ok=True)
    os.makedirs(mipmap_v26_dir, exist_ok=True)

    # Удаляем устаревшие VectorDrawable XML, вызывавшие сбои при инфлейте сторонними менеджерами
    for f in ["ic_launcher_background.xml", "ic_launcher_foreground.xml", "ic_launcher_monochrome.xml"]:
        fp = os.path.join(drawable_dir, f)
        if os.path.exists(fp):
            os.remove(fp)
            print(f"[REMOVED] Old vector: {f}")

    print("[1/3] Generating raster PNGs for all mipmap densities...")
    for density_name, sizes in DENSITIES.items():
        density_dir = os.path.join(res_dir, density_name)
        os.makedirs(density_dir, exist_ok=True)

        icon_w, icon_h = sizes["icon"]
        fg_w, fg_h = sizes["fg"]

        master_squircle.resize((icon_w, icon_h), Image.Resampling.LANCZOS).save(os.path.join(density_dir, "ic_launcher.png"))
        master_round.resize((icon_w, icon_h), Image.Resampling.LANCZOS).save(os.path.join(density_dir, "ic_launcher_round.png"))
        master_fg.resize((fg_w, fg_h), Image.Resampling.LANCZOS).save(os.path.join(density_dir, "ic_launcher_foreground.png"))
        print(f"  -> {density_name}: {icon_w}x{icon_h} (launcher/round), {fg_w}x{fg_h} (foreground)")

    print("[2/3] Generating drawable-nodpi, drawable fallback and root PNGs...")
    master_squircle.resize((512, 512), Image.Resampling.LANCZOS).save(os.path.join(PROJECT_ROOT, "icon.png"))
    master_squircle.resize((512, 512), Image.Resampling.LANCZOS).save(os.path.join(drawable_nodpi_dir, "ic_launcher.png"))
    master_round.resize((512, 512), Image.Resampling.LANCZOS).save(os.path.join(drawable_nodpi_dir, "ic_launcher_round.png"))
    master_fg.resize((512, 512), Image.Resampling.LANCZOS).save(os.path.join(drawable_nodpi_dir, "ic_launcher_foreground.png"))

    master_squircle.resize((192, 192), Image.Resampling.LANCZOS).save(os.path.join(drawable_dir, "ic_launcher.png"))
    master_round.resize((192, 192), Image.Resampling.LANCZOS).save(os.path.join(drawable_dir, "ic_launcher_round.png"))
    master_fg.resize((432, 432), Image.Resampling.LANCZOS).save(os.path.join(drawable_dir, "ic_launcher_foreground.png"))

    print("[3/3] Setting adaptive-icon XMLs with color background...")
    adaptive_xml = """<?xml version="1.0" encoding="utf-8"?>
<adaptive-icon xmlns:android="http://schemas.android.com/apk/res/android">
    <background android:drawable="@color/icon_background" />
    <foreground android:drawable="@mipmap/ic_launcher_foreground" />
</adaptive-icon>
"""
    with open(os.path.join(mipmap_v26_dir, "ic_launcher.xml"), "w", encoding="utf-8") as f:
        f.write(adaptive_xml)
    with open(os.path.join(mipmap_v26_dir, "ic_launcher_round.xml"), "w", encoding="utf-8") as f:
        f.write(adaptive_xml)

    colors_xml = """<?xml version="1.0" encoding="utf-8"?>
<resources>
    <color name="icon_background">#211B1A</color>
    <color name="ic_launcher_background">#211B1A</color>
</resources>
"""
    values_dir = os.path.join(res_dir, "values")
    os.makedirs(values_dir, exist_ok=True)
    with open(os.path.join(values_dir, "colors.xml"), "w", encoding="utf-8") as f:
        f.write(colors_xml)

    print("=== ICON GENERATION COMPLETE: 100% COVERAGE WITH USER IMAGE ===")

if __name__ == "__main__":
    main()
