import os
import time
import json
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine
from kafka import KafkaProducer

print("🚀 Iniciando Productor...")

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://root:root@host.docker.internal:5432/linkedin"
)
print(f"ℹ️ Conectando a base de datos: {DATABASE_URL.replace('root:root@', '****:****@')}")
engine = None
try:
    engine = create_engine(DATABASE_URL)
    with engine.connect() as connection:
        print("✅ Conexión a PostgreSQL exitosa.")
except Exception as e:
    print(f"❌ Error al conectar a PostgreSQL: {e}")
    exit(1)

try:
    print("ℹ️ Leyendo tabla 'merge.merge' desde PostgreSQL...")
    df = pd.read_sql_table("merge", schema="merge", con=engine)
    print(f"✅ Tabla 'merge.merge' leída. Número de filas: {len(df)}")
    if df.empty:
        print("⚠️ La tabla está vacía. No se enviarán mensajes.")
    else:
        print("📋 Primeras 3 filas (datos originales):")
        relevant_cols_preview = ['job_id', 'company_id', 'company_name', 'industry_category', 'state_only', 'normalized_salary', 'original_listed_month', 'original_listed_year', 'remote_allowed']
        print(df[relevant_cols_preview].head(3))
except Exception as e:
    print(f"❌ Error al leer la tabla 'merge.merge': {e}")
    exit(1)

print("ℹ️ Procesando timestamps y tipos de datos...")
df["original_listed_month"] = df["original_listed_month"].astype(str)
df["original_listed_year"] = df["original_listed_year"].astype(str)

df["event_timestamp_dt"] = pd.to_datetime(
    df["original_listed_month"].str.capitalize() + " " +
    df["original_listed_year"],
    format="%B %Y",
    errors="coerce"
)

valid_dates_mask = df["event_timestamp_dt"].notna()
df_valid_data = df[valid_dates_mask].copy()
df_invalid_dates_count = len(df) - len(df_valid_data)

if df_invalid_dates_count > 0:
    print(f"⚠️ Se encontraron {df_invalid_dates_count} filas con fechas inválidas que serán omitidas.")

if not df_valid_data.empty:
    df_valid_data["event_timestamp"] = df_valid_data["event_timestamp_dt"].astype("int64") // 10**9
    numeric_cols = ['company_id', 'company_size', 'employee_count', 'follower_count', 'views', 'applies', 'normalized_salary', 'len_description', 'benefits_count']
    boolean_cols = ['remote_allowed', 'has_benefits']

    for col in numeric_cols:
        if col in df_valid_data.columns:
            if df_valid_data[col].isnull().any():
                df_valid_data[col] = df_valid_data[col].fillna(0) 
            if pd.api.types.is_integer_dtype(df_valid_data[col].dtype) or col in ['company_id', 'company_size', 'employee_count', 'follower_count', 'views', 'applies', 'benefits_count']:
                df_valid_data[col] = df_valid_data[col].astype(int)
            else:
                df_valid_data[col] = df_valid_data[col].astype(float)


    for col in boolean_cols:
        if col in df_valid_data.columns:
            df_valid_data[col] = df_valid_data[col].fillna(0).astype(int)

    string_cols = ['company_name', 'formatted_work_type', 'application_type', 'formatted_experience_level',
                   'state_only', 'original_listed_month', 'industry_category', 'skills_list']
    for col in string_cols:
        if col in df_valid_data.columns:
            df_valid_data[col] = df_valid_data[col].fillna(f"unknown_{col}").astype(str)


    print("📋 Primeras 3 filas (datos listos para enviar):")
    cols_to_send_preview = ['job_id', 'event_timestamp', 'industry_category', 'state_only', 'normalized_salary', 'remote_allowed']
    print(df_valid_data[cols_to_send_preview].head(3))
else:
    print("⚠️ No hay filas con fechas válidas después del procesamiento.")
    df_valid_data = pd.DataFrame()

producer = None
KAFKA_BROKER_URL = "kafka:9092"
print(f"ℹ️ Conectando a Kafka broker: {KAFKA_BROKER_URL}")
max_retries = 5
retry_count = 0
while not producer and retry_count < max_retries:
    try:
        producer = KafkaProducer(
            bootstrap_servers=[KAFKA_BROKER_URL],
            value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8')
        )
        print("✅ Conectado a Kafka.")
    except Exception as e:
        retry_count += 1
        print(f"⏳ Kafka no disponible (intento {retry_count}/{max_retries}), reintentando en 5s… Error: {e}")
        if retry_count >= max_retries:
            print(f"❌ No se pudo conectar a Kafka después de {max_retries} intentos. Abortando.")
            exit(1)
        time.sleep(5)

print(f"ℹ️ Enviando mensajes al topic 'remote_offers'...")
messages_sent_count = 0

columns_to_send = [
    'job_id', 'company_id', 'company_name', 'company_size', 'employee_count',
    'follower_count', 'views', 'applies', 'formatted_work_type', 'remote_allowed',
    'application_type', 'formatted_experience_level', 'normalized_salary',
    'len_description', 'state_only', 'original_listed_month', 'original_listed_year',
    'has_benefits', 'benefits_count', 'industry_category', 'skills_list',
    'event_timestamp'
]

for _, row in df_valid_data.iterrows():
    msg = {}
    for col in columns_to_send:
        if col in row:
            msg[col] = row[col]
        else:
            print(f"⚠️ Columna '{col}' no encontrada en la fila, no se incluirá en el mensaje.")

    if 'event_timestamp' in msg:
        msg['timestamp'] = int(msg['event_timestamp']) 
        print(f"❌ Mensaje sin event_timestamp, saltando: {row['job_id']}")
        continue


    try:
        producer.send("remote_offers", msg)
        messages_sent_count += 1
        if messages_sent_count % 200 == 0 or messages_sent_count == 1:
             print(f"➡️ Mensaje {messages_sent_count} enviado: { {k: msg[k] for k in ['job_id', 'industry_category', 'state_only', 'normalized_salary', 'timestamp']} }")
    except Exception as e:
        print(f"❌ Error al enviar mensaje: {e}, Mensaje (parcial): { {k: msg.get(k) for k in ['job_id', 'industry_category']} }")
    time.sleep(0.01)

producer.flush()
print(f"🚽 Productor flushed.")
producer.close()
print(f"🔒 Productor cerrado.")
print(f"✅ Enviados {messages_sent_count} mensajes a 'remote_offers' de un total de {len(df_valid_data)} filas con datos válidos.")
if df_invalid_dates_count > 0:
    print(f"ℹ️ Se omitieron {df_invalid_dates_count} filas debido a fechas inválidas.")
print("🏁 Productor finalizado.")