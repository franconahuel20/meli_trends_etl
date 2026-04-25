# ================= IMPORTS =================
import os
import re
import csv
import time
from io import StringIO, BytesIO
from datetime import datetime, date

import boto3
import numpy as np
import pandas as pd
import pendulum
import requests
import psycopg2
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from sqlalchemy import create_engine, text
from thefuzz import process, fuzz
from unidecode import unidecode

# ================= CONFIG =================
LOCAL_TZ = pendulum.timezone("America/Argentina/Buenos_Aires")

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres").split(":")[0]
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")

POSTGRES_DB = os.getenv("POSTGRES_DB","snowflake_dev")
POSTGRES_USER = os.getenv("POSTGRES_USER","postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD","postgres")

POSTGRES_URI = f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"

MINIO_BUCKET = os.getenv("MINIO_BUCKET", "meli-datalake")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")

DATA_DIR = "/opt/airflow/data/input"

L1_PREFIX = "L1_delta/revenue_research_meli/trends"
L2_PREFIX = "L2_databases/revenue_research_meli/trends"
L3_PREFIX = "L3_standardization/revenue_research_meli/trends"

STANDARDIZATION_SCHEMA = "standardization_revenue_research_meli"
FINAL_STG_SCHEMA = "stg_revenue_research_meli"
FINAL_STG_TABLE = "meli_trends_l3"

# ================= HELPERS =================
def pg_engine():
    return create_engine(POSTGRES_URI)

def get_pg_conn():
    return psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD
    )

def s3_client():
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
    )

def normalize_text(v):
    if pd.isna(v): return ""
    v = unidecode(str(v).lower().strip())
    v = re.sub(r"[^\w\s-]", " ", v)
    v = re.sub(r"\s+", " ", v)
    return v.strip()

# ================= FIX EXCEL (CORREGIDO) =================
def load_excel_inputs_to_postgres():

    configs = {
        "brands.xlsx": {
            "table": "brands",
            "map": {
                "marca": "brand_str",
                "branded": "branded_int"
            }
        },
        "characters.xlsx": {
            "table": "characters",
            "map": {
                "n": "number_id_int",
                "grupo empresa owner derechos": "group_company_owner_rights_str",
                "twdc": "twdc_int",
                "franquicia n1": "franchise_n1_str",
                "franquicia n2": "franchise_n2_str",
                "franquicia n3": "franchise_n3_str",
                "contenido": "content_str",
                "personaje": "character_str",
                "personaje estandarizado": "character_standardization_str"
            }
        },
        "mercado_libre_subcat.xlsx": {
            "table": "mercado_libre_subcat",
            "map": {
                "cod_cat": "cat_code_str",
                "categoria_api": "category_api_str",
                "cod_subcat": "cod_subcat_str",
                "subcategoria": "subcategory_str",
                "items": "items_int",
                "pais": "country_str",
                "categoria_homologada": "homologated_category_str",
                "id_seleccionado": "id_selected_int",
                "subcategoria_homologada": "homologated_subcategory_str"
            }
        }
    }

    conn = get_pg_conn()
    cur = conn.cursor()

    cur.execute(f"CREATE SCHEMA IF NOT EXISTS {STANDARDIZATION_SCHEMA}")

    for file, cfg in configs.items():
        path = os.path.join(DATA_DIR, file)

        df = pd.read_excel(path)

        # 🔍 DEBUG
        print(f"\nProcesando archivo: {file}")
        print("COLUMNAS ORIGINALES:", df.columns.tolist())

        # normalizar columnas
        df.columns = [normalize_text(str(c)) for c in df.columns]

        print("COLUMNAS NORMALIZADAS:", df.columns.tolist())

        # 🔥 FIX DEFINITIVO: mapear SOLO columnas existentes
        df_renamed = {}

        for original, target in cfg["map"].items():
            if original in df.columns:
                df_renamed[target] = df[original]

        df = pd.DataFrame(df_renamed)

        print("COLUMNAS FINALES:", df.columns.tolist())
        print("FILAS:", len(df))

        if df.empty:
            raise ValueError(f"El archivo {file} no tiene datos válidos después del mapping")

        table = f"{STANDARDIZATION_SCHEMA}.{cfg['table']}"

        # recrear tabla
        cur.execute(f"DROP TABLE IF EXISTS {table}")

        cols_sql = ", ".join([f"{c} TEXT" for c in df.columns])
        cur.execute(f"CREATE TABLE {table} ({cols_sql})")

        # insertar datos
        buffer = StringIO()
        df.to_csv(buffer, index=False, header=False)
        buffer.seek(0)

        cur.copy_expert(f"COPY {table} FROM STDIN WITH CSV", buffer)

        conn.commit()

        print(f"{file} cargado correctamente")

    cur.close()
    conn.close()
# ================= RESTO SIN CAMBIOS =================

def get_meli_access_token(**context):
    r = requests.post("https://api.mercadolibre.com/oauth/token", data={
        "grant_type":"client_credentials",
        "client_id":os.getenv("ML_CLIENT_ID"),
        "client_secret":os.getenv("ML_CLIENT_SECRET")
    })
    r.raise_for_status()
    context["ti"].xcom_push(key="access_token", value=r.json()["access_token"])

def read_subcategory_parameters():
    conn = get_pg_conn()
    cur = conn.cursor()

    cur.execute(f"""
        SELECT country_str,
               cod_subcat_str,
               homologated_category_str,
               homologated_subcategory_str
        FROM {STANDARDIZATION_SCHEMA}.mercado_libre_subcat
    """)

    rows = cur.fetchall()

    cols = [
        "country_str",
        "cod_subcat_str",
        "homologated_category_str",
        "homologated_subcategory_str"
    ]

    df = pd.DataFrame(rows, columns=cols)

    cur.close()
    conn.close()

    return df

def extract_meli_trends_to_l1(**context):
    import logging

    logger = logging.getLogger("airflow.task")

    token = context["ti"].xcom_pull(task_ids="get_meli_access_token", key="access_token")

    if not token:
        raise ValueError("No se obtuvo access_token desde XCom.")

    df_params = read_subcategory_parameters()

    logger.info("Total filas leídas desde mercado_libre_subcat: %s", len(df_params))

    if df_params.empty:
        raise ValueError("read_subcategory_parameters devolvió 0 filas.")

    df_params = df_params[
        df_params["cod_subcat_str"].notna()
        & (df_params["cod_subcat_str"].astype(str).str.strip() != "")
    ].copy()

    logger.info("Filas válidas con cod_subcat_str: %s", len(df_params))

    if df_params.empty:
        raise ValueError("No hay cod_subcat_str válidos para consultar Mercado Libre.")

    rows = []
    total_calls = 0
    total_success = 0
    total_failed = 0

    for _, r in df_params.iterrows():
        cod_subcat = str(r["cod_subcat_str"]).strip()

        if len(cod_subcat) < 4:
            logger.warning("Subcategoría inválida ignorada: %s", cod_subcat)
            continue

        site_id = cod_subcat[:3]
        url = f"https://api.mercadolibre.com/trends/{site_id}/{cod_subcat}"

        try:
            response = requests.get(
                url,
                headers={"Authorization": f"Bearer {token}"},
                timeout=60,
            )

            total_calls += 1

            logger.info("Calling URL: %s | Status: %s", url, response.status_code)

            if response.status_code != 200:
                total_failed += 1
                logger.warning(
                    "API failed for cod_subcat=%s | status=%s | response=%s",
                    cod_subcat,
                    response.status_code,
                    response.text[:500],
                )
                continue

            payload = response.json() or []

            if not payload:
                logger.warning("API returned empty payload for cod_subcat=%s", cod_subcat)
                continue

            total_success += 1

            for item in payload:
                rows.append({
                    "Keyword": item.get("keyword"),
                    "URL": item.get("url"),
                    "Date": date.today().isoformat(),
                    "Country": r["country_str"],
                    "Category": r["homologated_category_str"],
                    "Subcategory": r["homologated_subcategory_str"],
                })

        except Exception as exc:
            total_failed += 1
            logger.exception("Request failed for cod_subcat=%s | error=%s", cod_subcat, exc)
            continue

    logger.info("Total API calls: %s", total_calls)
    logger.info("Successful API calls: %s", total_success)
    logger.info("Failed API calls: %s", total_failed)
    logger.info("Rows generated: %s", len(rows))

    if not rows:
        raise ValueError(
            "No se obtuvieron datos desde Mercado Libre API. "
            f"total_calls={total_calls}, success={total_success}, failed={total_failed}"
        )

    df = pd.DataFrame(
        rows,
        columns=["Keyword", "URL", "Date", "Country", "Category", "Subcategory"],
    )

    today_ddmmyyyy = datetime.now(LOCAL_TZ).strftime("%d%m%Y")
    filename = f"mercado_libre_trends_{today_ddmmyyyy}.csv"
    key = f"{L1_PREFIX}/{filename}"

    csv_buffer = StringIO()
    df.to_csv(csv_buffer, sep=";", index=False, quoting=csv.QUOTE_MINIMAL, encoding="utf-8")

    s3_client().put_object(
        Bucket=MINIO_BUCKET,
        Key=key,
        Body=csv_buffer.getvalue().encode("utf-8"),
        ContentType="text/csv; charset=utf-8",
    )

    logger.info("Saved L1 CSV: s3://%s/%s | rows=%s", MINIO_BUCKET, key, len(df))

    context["ti"].xcom_push(key="l1_key", value=key)

def transform_l1_to_l2(**context):
    key=context["ti"].xcom_pull(task_ids="extract_meli_trends_to_l1",key="l1_key")
    obj=s3_client().get_object(Bucket=MINIO_BUCKET,Key=key)

    df=pd.read_csv(obj["Body"],sep=";")
    df.columns=["keyword_str","url_str","date_dte","country_str","category_str","subcategory_str"]

    buffer=BytesIO()
    df.to_parquet(buffer,index=False)

    l2_key=f"{L2_PREFIX}/data.parquet"
    s3_client().put_object(Bucket=MINIO_BUCKET,Key=l2_key,Body=buffer.getvalue())

    context["ti"].xcom_push(key="l2_key",value=l2_key)

def fuzzy_match_l2_to_l3(**context):
    from collections import defaultdict

    # ================= LOAD L2 =================
    key = context["ti"].xcom_pull(task_ids="transform_l1_to_l2", key="l2_key")
    obj = s3_client().get_object(Bucket=MINIO_BUCKET, Key=key)
    df = pd.read_parquet(BytesIO(obj["Body"].read()))

    conn = get_pg_conn()

    # ================= LOAD MASTER DATA =================
    df_char = pd.read_sql(f"""
        SELECT *
        FROM {STANDARDIZATION_SCHEMA}.characters
        WHERE character_str IS NOT NULL
    """, conn)

    df_brand = pd.read_sql(f"""
        SELECT *
        FROM {STANDARDIZATION_SCHEMA}.brands
        WHERE brand_str IS NOT NULL
    """, conn)

    conn.close()

    # ================= NORMALIZATION =================
    df["keyword_clean"] = df["keyword_str"].apply(normalize_text)

    df_char["character_clean"] = df_char["character_str"].apply(normalize_text)
    df_brand["brand_clean"] = df_brand["brand_str"].apply(normalize_text)

    characters = df_char["character_clean"].tolist()
    brands = df_brand["brand_clean"].tolist()

    # ================= MATCHING =================
    results = defaultdict(list)

    for kw in df["keyword_clean"]:

        best_char = None
        best_brand = None

        # CHARACTER MATCH
        char_match = process.extractOne(kw, characters, scorer=fuzz.token_set_ratio)

        if char_match and char_match[1] >= 75:
            row = df_char[df_char["character_clean"] == char_match[0]].iloc[0]
            best_char = row

        # BRAND MATCH
        brand_match = process.extractOne(kw, brands, scorer=fuzz.token_set_ratio)

        if brand_match and brand_match[1] >= 75:
            row_b = df_brand[df_brand["brand_clean"] == brand_match[0]].iloc[0]
            best_brand = row_b

        # character fields
        if best_char is not None:
            results["character_str"].append(best_char.get("character_str"))
            results["franchise_n1_str"].append(best_char.get("franchise_n1_str"))
            results["franchise_n2_str"].append(best_char.get("franchise_n2_str"))
            results["franchise_n3_str"].append(best_char.get("franchise_n3_str"))
            results["content_str"].append(best_char.get("content_str"))
            results["group_company_owner_rights_str"].append(best_char.get("group_company_owner_rights_str"))
            results["twdc_int"].append(best_char.get("twdc_int"))
        else:
            results["character_str"].append(None)
            results["franchise_n1_str"].append(None)
            results["franchise_n2_str"].append(None)
            results["franchise_n3_str"].append(None)
            results["content_str"].append(None)
            results["group_company_owner_rights_str"].append(None)
            results["twdc_int"].append(None)

        # brand fields
        if best_brand is not None:
            results["brand_str"].append(best_brand.get("brand_str"))
        else:
            results["brand_str"].append(None)

    # ================= MERGE RESULTS =================
    for col, values in results.items():
        df[col] = values

    # ================= EXTRA FEATURES =================
    df["loading_dtm"] = datetime.now()
    df["file_row_id_int"] = range(1, len(df) + 1)

    df["year_calendar_int"] = pd.to_datetime(df["date_dte"], errors="coerce").dt.year
    df["month_calendar_int"] = pd.to_datetime(df["date_dte"], errors="coerce").dt.month
    df["day_calendar_int"] = pd.to_datetime(df["date_dte"], errors="coerce").dt.day

    # ================= SAVE TO POSTGRES =================
    conn = get_pg_conn()
    cur = conn.cursor()

    cur.execute(f"CREATE SCHEMA IF NOT EXISTS {FINAL_STG_SCHEMA}")
    cur.execute(f"DROP TABLE IF EXISTS {FINAL_STG_SCHEMA}.{FINAL_STG_TABLE}")

    cols_sql = ", ".join([f"{c} TEXT" for c in df.columns])
    cur.execute(f"CREATE TABLE {FINAL_STG_SCHEMA}.{FINAL_STG_TABLE} ({cols_sql})")

    buffer = StringIO()
    df.to_csv(buffer, index=False, header=False)
    buffer.seek(0)

    cur.copy_expert(f"COPY {FINAL_STG_SCHEMA}.{FINAL_STG_TABLE} FROM STDIN WITH CSV", buffer)

    conn.commit()
    cur.close()
    conn.close()

# ================= DAG =================
with DAG(
    dag_id="meli_trends_end_to_end",
    schedule="0 23 * * *",
    start_date=datetime(2025,1,1,tzinfo=LOCAL_TZ),
    catchup=False
) as dag:

    load_seed_tables = PythonOperator(
        task_id="load_excel_inputs_to_postgres",
        python_callable=load_excel_inputs_to_postgres
    )

    get_token = PythonOperator(
        task_id="get_meli_access_token",
        python_callable=get_meli_access_token
    )

    extract_l1 = PythonOperator(
        task_id="extract_meli_trends_to_l1",
        python_callable=extract_meli_trends_to_l1
    )

    transform_l2 = PythonOperator(
        task_id="transform_l1_to_l2",
        python_callable=transform_l1_to_l2
    )

    match_l3 = PythonOperator(
        task_id="fuzzy_match_l2_to_l3",
        python_callable=fuzzy_match_l2_to_l3
    )

    run_dbt = BashOperator(
    task_id="run_dbt_models",
    bash_command="""
    cd /opt/airflow/dbt && \
    rm -rf target && \
    dbt deps && \
    dbt run
    """
    )

    load_seed_tables >> get_token >> extract_l1 >> transform_l2 >> match_l3 >> run_dbt