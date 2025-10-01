import os, io, json, time, sys, shutil, pathlib
import httpx
import pandas as pd

# ENV (configurar como Secrets en el repo)
REGISTRY_SHEET_CSV = os.environ["REGISTRY_SHEET_CSV"]  # Link CSV (Publish to the web) del Registry
PAGES_DIR = "public"                                   # Se publica a GitHub Pages
BOT_REFRESH_URL = os.environ.get("BOT_REFRESH_URL")    # https://<bot>.onrender.com/admin/refresh-all
ADMIN_TOKEN = os.environ.get("ADMIN_TOKEN")            # token compartido con el bot

def ensure_clean(dir_path: str):
    p = pathlib.Path(dir_path)
    if p.exists():
        shutil.rmtree(p)
    p.mkdir(parents=True, exist_ok=True)

async def fetch_csv_as_df(url: str) -> pd.DataFrame:
    async with httpx.AsyncClient(timeout=45) as client:
        r = await client.get(url)
        r.raise_for_status()
        return pd.read_csv(io.StringIO(r.text))

def sheet_csv_url(doc_id: str, sheet_name: str) -> str:
    # Usa nombre de hoja, sin depender de gid
    return f"https://docs.google.com/spreadsheets/d/{doc_id}/gviz/tq?tqx=out:csv&sheet={sheet_name}"

def normalize_faqs(df: pd.DataFrame):
    req = ["intent","keywords","answer"]
    for c in req:
        if c not in df.columns:
            raise ValueError(f"FAQs: falta columna {c}")
    rows = []
    for _,row in df.iterrows():
        rows.append({
            "intent": str(row.get("intent","")).strip(),
            "keywords": str(row.get("keywords","")).strip(),
            "answer": str(row.get("answer","")).strip(),
        })
    return {"rows": rows, "generated_at": int(time.time())}

def normalize_promos(df: pd.DataFrame):
    req = ["name","trigger_keywords","message","start_date","end_date"]
    for c in req:
        if c not in df.columns:
            raise ValueError(f"Promos: falta columna {c}")
    rows = []
    for _,row in df.iterrows():
        rows.append({
            "name": str(row.get("name","")).strip(),
            "trigger_keywords": str(row.get("trigger_keywords","")).strip(),
            "message": str(row.get("message","")).strip(),
            "start_date": str(row.get("start_date","")).strip(),
            "end_date": str(row.get("end_date","")).strip(),
        })
    return {"rows": rows, "generated_at": int(time.time())}

async def main():
    ensure_clean(PAGES_DIR)
    routing = {}     # wa_id -> slug
    slugs_meta = {}  # slug -> {"faqs_url":..., "promos_url":...}
    generated_at = int(time.time())

    # 1) Leer Registry
    df_reg = await fetch_csv_as_df(REGISTRY_SHEET_CSV)
    required = ["slug","wa_id","doc_id","faqs_sheet_name","promos_sheet_name"]
    for c in required:
        if c not in df_reg.columns:
            raise ValueError(f"Registry: falta columna {c}")
    # locales_sheet_name es opcional; display_name/folder opcionales

    # 2) Por cada local
    for _, row in df_reg.iterrows():
        slug = str(row.get("slug","")).strip()
        wa_id = str(row.get("wa_id","")).strip()
        doc_id = str(row.get("doc_id","")).strip()
        faqs_name = (str(row.get("faqs_sheet_name","faqs")).strip() or "faqs")
        promos_name = (str(row.get("promos_sheet_name","promos")).strip() or "promos")
        locales_name = str(row.get("locales_sheet_name","")).strip()  # opcional

        if not slug or not wa_id or not doc_id:
            continue

        routing[wa_id] = slug

        # Construir URLs CSV por nombre de hoja
        faqs_csv   = sheet_csv_url(doc_id, faqs_name)
        promos_csv = sheet_csv_url(doc_id, promos_name)

        # Descargar
        df_faqs   = await fetch_csv_as_df(faqs_csv)
        df_promos = await fetch_csv_as_df(promos_csv)

        # Normalizar
        faqs_json   = normalize_faqs(df_faqs)
        promos_json = normalize_promos(df_promos)

        # locales (opcional): agrega promos por sede o cadena
        if locales_name:
            try:
                locales_csv = sheet_csv_url(doc_id, locales_name)
                df_loc = await fetch_csv_as_df(locales_csv)
                # columnas esperadas: target_slug,name,trigger_keywords,message,start_date,end_date
                if all(col in df_loc.columns for col in
                       ["target_slug","name","trigger_keywords","message","start_date","end_date"]):
                    # filtrar por target_slug == slug o '*'
                    df_extra = df_loc[(df_loc["target_slug"]==slug) | (df_loc["target_slug"]=="*")].copy()
                    if not df_extra.empty:
                        df_extra = df_extra[["name","trigger_keywords","message","start_date","end_date"]]
                        # concatenar
                        df_concat = pd.concat([pd.DataFrame(promos_json["rows"]), df_extra], ignore_index=True)
                        promos_json = normalize_promos(df_concat)  # re-normaliza
            except Exception as e:
                print(f"[WARN] locales no aplicadas para slug={slug}: {e}", file=sys.stderr)

        # 3) Escribir JSON por slug
        slug_dir = os.path.join(PAGES_DIR, "slugs", slug)
        os.makedirs(slug_dir, exist_ok=True)
        with open(os.path.join(slug_dir, "faqs.json"), "w", encoding="utf-8") as f:
            json.dump(faqs_json, f, ensure_ascii=False)
        with open(os.path.join(slug_dir, "promos.json"), "w", encoding="utf-8") as f:
            json.dump(promos_json, f, ensure_ascii=False)

        # URLs relativas (GitHub Pages)
        slugs_meta[slug] = {
            "faqs_url": f"./slugs/{slug}/faqs.json",
            "promos_url": f"./slugs/{slug}/promos.json"
        }

    # 4) index.json
    index = {"generated_at": generated_at, "routing": routing, "slugs": slugs_meta}
    with open(os.path.join(PAGES_DIR, "index.json"), "w", encoding="utf-8") as f:
        json.dump(index, f, ensure_ascii=False)

    # 5) refrescar bot
    if BOT_REFRESH_URL and ADMIN_TOKEN:
        try:
            async with httpx.AsyncClient(timeout=20) as client:
                r = await client.post(f"{BOT_REFRESH_URL}?admin_token={ADMIN_TOKEN}")
                print("Refresh status:", r.status_code, r.text[:200])
        except Exception as e:
            print("No se pudo refrescar el bot:", e, file=sys.stderr)

if __name__ == "__main__":
    import asyncio
    asyncio.run(main())
