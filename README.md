# KitchenMasterRecipes

Automated daily recipe feed and translation pipeline for KitchenMaster.

## Overview
- **Pipeline:** High-throughput async ingestion from TheMealDB and Russian open sources.
- **Translation:** Deep translation and localization of culinary units using Google Translate (GTX) with persistent caching.
- **Classification:** Strict category classification (First, Second, Salad, Dessert, Breakfast).
- **Distribution:** Daily delta chunks and aggregated feed with SHA-256 integrity verification.

## Endpoints
- Manifest: `https://raw.githubusercontent.com/Outrot174/KitchenMasterRecipes/main/data/manifest.json`
- Full Feed: `https://raw.githubusercontent.com/Outrot174/KitchenMasterRecipes/main/data/recipes_feed.json`
- Daily Chunks: `https://raw.githubusercontent.com/Outrot174/KitchenMasterRecipes/main/data/daily/YYYY-MM-DD.json`
