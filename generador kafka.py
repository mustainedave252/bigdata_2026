
import csv
import json
import time
from datetime import datetime
from kafka import KafkaProducer

# Configuración del productor Kafka
productor = KafkaProducer(
    bootstrap_servers=["localhost:9092"],
    value_serializer=lambda dato: json.dumps(
        dato, ensure_ascii=False
    ).encode("utf-8")
)

# Configuración del archivo y tópico
archivo_csv = "/home/destino/NetFlix.csv"
topico = "netflix_events"

total_eventos = 0

try:
    with open(
        archivo_csv,
        mode="r",
        encoding="utf-8-sig",
        newline=""
    ) as archivo:

        lector = csv.DictReader(archivo)

        for fila in lector:
            evento = {
                "evento": "catalogo_item",
                "show_id": fila.get("show_id", ""),
                "type": fila.get("type", ""),
                "title": fila.get("title", ""),
                "release_year": fila.get("release_year", ""),
                "rating": fila.get("rating", ""),
                "genres": fila.get("listed_in", ""),
                "duration": fila.get("duration", ""),
                "event_time": datetime.now().isoformat()
            }

            productor.send(topico, value=evento)
            productor.flush()

            total_eventos += 1

            print(
                f"Evento {total_eventos}: "
                f"{evento['type']} - {evento['title']}"
            )

            # Simular la llegada progresiva de eventos
            time.sleep(1)

except KeyboardInterrupt:
    print("\nGeneración detenida por el usuario.")

finally:
    productor.close()
    print(f"Total de eventos enviados: {total_eventos}")
