# restaurant-updater

Lee un Registry (Google Sheets → CSV) con columnas:
- slug, wa_id, doc_id, faqs_sheet_name, promos_sheet_name
- locales_sheet_name (opcional), display_name (opcional), folder (opcional)

Por cada fila:
- Construye CSV por `doc_id + sheet=faqs|promos|locales` (sin gid).
- Genera `public/slugs/<slug>/faqs.json` y `promos.json`.
- Publica `public/index.json` con routing `wa_id -> slug`.
- Llama al bot `/admin/refresh-all?admin_token=...`.

## Configurar
- Settings → Secrets and variables → Actions:
  - REGISTRY_SHEET_CSV
  - BOT_REFRESH_URL
  - ADMIN_TOKEN

## GitHub Pages
- Settings → Pages → Build and deployment: GitHub Actions (workflow ya incluido).
