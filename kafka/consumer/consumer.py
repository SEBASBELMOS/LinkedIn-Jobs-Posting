import json
import time
import collections
from datetime import datetime, timedelta
from kafka import KafkaConsumer
import pandas as pd

print("🚀 Iniciando Consumidor...")

consumer = None
KAFKA_BROKER_URL = "kafka:9092"
KAFKA_TOPIC = "remote_offers"
print(f"ℹ️ Conectando a Kafka broker: {KAFKA_BROKER_URL}, Topic: {KAFKA_TOPIC}")
max_retries = 5
retry_count = 0
while not consumer and retry_count < max_retries:
    try:
        consumer = KafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=[KAFKA_BROKER_URL],
            auto_offset_reset="earliest",
            value_deserializer=lambda m: json.loads(m.decode('utf-8'))
        )
        print("✅ Consumidor conectado a Kafka.")
    except Exception as e:
        retry_count += 1
        print(f"⏳ Error al conectar Kafka (intento {retry_count}/{max_retries}), reintentando en 5s… Error: {e}")
        if retry_count >= max_retries:
            print(f"❌ No se pudo conectar a Kafka después de {max_retries} intentos. Abortando.")
            exit(1)
        time.sleep(5)


WINDOW_SEC = 60
window = collections.deque()
output_file_path = "/app/data/current_count.json"

def top_items_from_window(win, key_name, N=5, default_value="unknown"):
    counts = {}
    for _, msg_data in win:
        item_value = msg_data.get(key_name, default_value)
        if pd.isna(item_value) or str(item_value).strip() == "":
            item_value = default_value
        counts[item_value] = counts.get(item_value, 0) + 1
    return sorted(counts.items(), key=lambda x: x[1], reverse=True)[:N]

print(f"👂 Escuchando mensajes en '{KAFKA_TOPIC}'. Ventana de {WINDOW_SEC} segundos.")
message_count_total = 0
try:
    for msg in consumer:
        message_count_total += 1
        data = msg.value
        processing_dt = datetime.utcnow()
        window.append((processing_dt, data))
        
        cutoff = processing_dt - timedelta(seconds=WINDOW_SEC)
        
        removed_count = 0
        while window and window[0][0] < cutoff:
            window.popleft()
            removed_count += 1
        

        top_N_industries_list = top_items_from_window(window, key_name="industry_category", N=5, default_value="unknown_industry")
        
        top_N_states_list = top_items_from_window(window, key_name="state_only", N=5, default_value="unknown_state")

        salaries = [d['normalized_salary'] for _, d in window if 'normalized_salary' in d and pd.notna(d.get('normalized_salary'))]
        
        out = {
            "count": len(window),
            "last_seen_processing_time": window[-1][0].isoformat() if window else None,
            "avg_salary_in_window": sum(salaries) / len(salaries) if salaries else 0,
            "top_industries": [{"industry": item, "count": c} for item, c in top_N_industries_list],
            "top_states": [{"state": item, "count": c} for item, c in top_N_states_list]
        }
        
        if message_count_total % 100 == 0:
            print(f"📝 Escribiendo a JSON ({output_file_path}): {json.dumps(out, indent=2)}")

        try:
            with open(output_file_path, "w") as f:
                json.dump(out, f, indent=2)
        except Exception as e:
            print(f"❌ Error al escribir en {output_file_path}: {e}")

except KeyboardInterrupt:
    print("\n🛑 Consumidor detenido por el usuario.")
except Exception as e:
    print(f"❌ Error inesperado en el bucle del consumidor: {e}")
finally:
    if consumer:
        consumer.close()
        print("🔒 Consumidor cerrado.")
    print("🏁 Consumidor finalizado.")