# YouTube Watch History ETL Pipeline

A **personalized Data Engineering project** that extracts, transforms, and analyzes your YouTube watch history using a modern **Data Lake + Airflow + AWS Glue + Athena** architecture.

This project demonstrates **end-to-end ETL, cloud integration, and data analytics skills** in a real-world context using your own data, making it a standout project for interviews and resumes.

---

## 🌟 Highlights

* **Personalized Dataset:** Uses your own YouTube watch history JSON from Google Takeout.
* **End-to-End ETL:** Extract → Transform → Load into S3 Data Lake.
* **Cloud Integration:** AWS S3, Glue, and Athena.
* **Automated Workflow:** Fully orchestrated using **Airflow DAGs**.
* **Data Analytics:** Athena SQL queries reveal viewing trends, top channels, and activity patterns.
* **Resume-Ready:** Realistic architecture, modular code, and reusable components.

---

## 🏗 Architecture

```
watch-history.json (local)
         │
         ▼
Airflow DAG: check_file → transform
         │
         ▼
Local processed files (CSV / Parquet)
         │
         ▼
Upload to S3:
  ├─ raw/
  ├─ staging/
  └─ curated/
         │
         ▼
AWS Glue Crawler → Glue Data Catalog (auto infer schema)
         │
         ▼
Athena → Run SQL queries → Query results in S3
```

* **Raw Layer:** Original JSON for archival.
* **Staging Layer:** Cleaned CSV for quick preview.
* **Curated Layer:** Partitioned Parquet for analytics.
* **Glue Crawler:** Auto-detect schema & partitions for Athena.
* **Athena:** SQL-based analytics for personalized trends.

---
## 🖼 Screenshots / Visualization

* **S3 Bucket:** Folders `raw/`, `staging/`, `curated/`
<img width="940" height="357" alt="image" src="https://github.com/user-attachments/assets/7472f751-289c-4380-b988-ebb379e465ce" />

* **Glue Crawler:** `youtube_watch_history_crawler` (status: READY)
<img width="940" height="406" alt="image" src="https://github.com/user-attachments/assets/8b38d1aa-d798-45e8-a244-a402c6d4808d" />

* **Athena:** Query result table preview for `curated_youtube_watch_data`
<img width="940" height="332" alt="image" src="https://github.com/user-attachments/assets/69bef4e9-f96e-4b1f-9bc8-3a4d7236fea4" />

* **Apache Airflow:** Dag setup
<img width="940" height="368" alt="image" src="https://github.com/user-attachments/assets/6d811d1e-66c0-470d-bc97-c78b193e451d" />



## 🛠 Tools & Technologies

| Layer             | Tool                                  |
| ----------------- | ------------------------------------- |
| Orchestration     | Apache Airflow (Docker Compose)       |
| Storage           | AWS S3 (Raw / Staging / Curated)      |
| ETL               | Python (pandas, JSON processing)      |
| Metadata / Schema | AWS Glue Crawler                      |
| Analytics         | Amazon Athena                         |
| Personalization   | YouTube API v3 (optional enhancement) |

---

## 🚀 Features

1. **Automated ETL Pipeline:**

   * Checks if `watch-history.json` exists
   * Transforms into structured CSV and Parquet
   * Uploads files to respective S3 layers

2. **Glue Integration:**

   * Runs crawler to infer schema
   * Automatically updates Athena table

3. **Athena Analytics:**

   * Top Channels & Top Videos
   * Monthly & Daily watch trends
   * Most active days/hours

4. **Reusable Components:**

   * Upload files to S3 (local path + S3 key)
   * Transform script modular for multiple outputs
   * Athena query runner handles multiple SQL queries

---

## 🗂 Folder Structure

```
dags/
├── youtube_monthly_etl.py         # Main DAG
└── scripts/
    ├── check_file.py              # Verify input file
    ├── transform_watch.py         # Transform JSON → CSV/Parquet
    ├── upload_to_s3.py            # Reusable S3 upload
    ├── run_glue_crawler.py        # Glue crawler runner
    ├── run_athena_queries.py      # Run Athena queries
    ├── include/
    │   └── watch-history.json    # Input JSON
    └── data/                      # Local staging & curated outputs
```

---

## ⚡ Setup & Run

1. **Clone repository**
2. **Configure AWS credentials** in `.env`:

   ```
   AWS_ACCESS_KEY_ID=YOUR_KEY
   AWS_SECRET_ACCESS_KEY=YOUR_SECRET
   AWS_REGION=ap-south-1
   ```
3. **Start Airflow** using Docker Compose:

   ```bash
   docker-compose up -d
   ```
4. **Upload `watch-history.json`** to `scripts/include/`
5. **Trigger DAG** from Airflow UI: `youtube_monthly_etl`
6. **View outputs**:

   * S3: `raw/`, `staging/`, `curated/`
   * Athena: Query results in `s3://youtube-watch-history-etl/athena-results/`

---

## 📊 Sample Queries

* **Top 5 Channels Watched**

```sql
SELECT channel_name, COUNT(*) AS videos_watched
FROM curated_youtube_watch_data
GROUP BY channel_name
ORDER BY videos_watched DESC
LIMIT 5;
```

* **Monthly Watch Trends**

```sql
SELECT year, month, COUNT(*) AS total_videos
FROM curated_youtube_watch_data
GROUP BY year, month
ORDER BY year DESC, month DESC;
```

* **Most Active Days**

```sql
SELECT day_of_week, COUNT(*) AS views
FROM curated_youtube_watch_data
GROUP BY day_of_week
ORDER BY views DESC;
```

---






