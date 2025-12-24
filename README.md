# Distributed Log Aggregator (FastAPI + Postgres + Docker + k6 + Pytest)

Project ini adalah implementasi sistem **log aggregator terdistribusi** dengan fitur:

- FastAPI sebagai event aggregator
- Postgres sebagai penyimpanan persisten
- Deduplication event berdasarkan `event_id`
- Batch processing
- Concurrency-safe insert
- Event replay (`GET /events?topic=X`)
- Stats endpoint (`/stats`)
- Load Testing menggunakan k6 (≥ 20.000 events)
- Integration + Unit Testing (20 pytest tests)
- Docker Compose multi-service (storage, aggregator, publisher)

---

## 🚀 Arsitektur Sistem

publisher → aggregator → postgres(storage)
__ k6 load test (external)


- **publisher** mengirim event secara terus-menerus.
- **aggregator** menerima, dedup, dan menyimpan.
- **storage** menyimpan log secara persisten.
- **tests** berisi 20 pengujian fungsional dan reliability.
- **k6** menguji throughput dan performa.

---

## 🏗 Cara Menjalankan

### 1. Build dan start seluruh stack
```bash
docker compose up --build