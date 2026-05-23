# iot-ems-data-pipeline

End-to-end time-series data pipeline for IoT-based energy management: data ingestion, cleaning, aggregation, and analytics using InfluxDB, Grafana, and Python.

<!-- KOMENTARZ -->

### 🥉 1. Bronze Layer (Edge Ingestion & Transport)

- **Data Source & Polling:** The Grenton GateModbus module acts as an industrial gateway, polling raw telemetry from the Huawei SUN2000 inverter and DTSU666 smart meter registers via Modbus TCP every 60 seconds.
- **Edge Processing:** Raw metrics are mapped into Grenton user features (variables).
- **Custom Transport Script (`sInfluxDB_post`):** A custom embedded script written for the Grenton GateHttp module dynamically handles authentication tokens, configures HTTP headers, structures the body into time-series line protocol, and pushes data natively via HTTP POST directly to the InfluxDB Cloud API.
- **Data Integrity Rule:** Building consumption calculations are strictly prohibited at the edge layer to maintain unmanipulated raw data traceability.

```mermaid
graph TD
    %% Definicja stylów i kolorów warstw
    classDef edgeStyle fill:#ececff,stroke:#9370db,stroke-width:2px,color:#000;
    classDef bronzeStyle fill:#ffd1b3,stroke:#ff6600,stroke-width:2px,color:#000;
    classDef silverStyle fill:#e6f2ff,stroke:#0066cc,stroke-width:2px,color:#000;
    classDef goldStyle fill:#ffffcc,stroke:#cca300,stroke-width:2px,color:#000;
    classDef archiveStyle fill:#d9f2d9,stroke:#2d862d,stroke-width:2px,color:#000;
    classDef vizStyle fill:#f2e6ff,stroke:#7a00cc,stroke-width:2px,color:#000;

        %% WARSTWA EDGE
        subgraph EDGE_LAYER ["🏠 Smart Home & Inverter Edge"]
            D[Huawei SUN2000 + DTSU666] -->|Modbus TCP| GM[Grenton GateModbus]
            GM --> GH[Grenton GateHttp <br> Transport Gateway]
        end
        class EDGE_LAYER,D,GM,GH edgeStyle;

        %% WARSTWA BRONZE
        subgraph BRONZE_LAYER ["🥉 Bronze Layer (Raw Storage)"]
            INF_B[(InfluxDB Cloud <br> measurement: power & energy)]
        end
        GH -->|HttpRequest via REST API <br> Over Internet <br> (pushed in 1-minute Intervals)| INF_B
        class BRONZE_LAYER,INF_B bronzeStyle;

        %% WARSTWA SILVER
        subgraph SILVER_LAYER ["🥈 Silver Layer (Clean Data)"]
            GHA_S[GitHub Actions <br> silver_layer.py] -->|InfluxDBClient API| INF_S[(InfluxDB Cloud <br> measurement: power_clean <br> & energy_clean)]
        end
        INF_B -->|YAML Trigger| GHA_S
        class SILVER_LAYER,GHA_S,INF_S silverStyle;

        %% WARSTWA GOLD
        subgraph GOLD_LAYER ["🥇 Gold Layer (Business Analytics)"]
            GHA_G1[GitHub Actions <br> gold_aggregator.py] -->|InfluxDBClient API| INF_G1[(InfluxDB Cloud <br> measurement: power_hourly <br> & energy_hourly)]
            GHA_G2[GitHub Actions <br> gold_battery_sim.py] -->|InfluxDBClient API| INF_G2[(InfluxDB Cloud <br> measurement: battery_simulation)]
        end
        INF_S -->|YAML Trigger| GHA_G1
        INF_S -->|YAML Trigger| GHA_G2
        class GOLD_LAYER,GHA_G1,INF_G1,GHA_G2,INF_G2 goldStyle;

        %% WARSTWA COLD STORAGE (ARCHIVE)
        subgraph COLD_STORAGE ["💾 Long-Term Cold Storage"]
            GHA_A[GitHub Actions <br> gold_daily_archive.py] -->|to_csv @Pandas| CSV[daily_ems_report.csv <br> Metrics & Savings in PLN]
        end
        INF_G1 -->|Daily Cron 00:05 UTC| GHA_A
        INF_G2 -->|Daily Cron 00:05 UTC| GHA_A
        class COLD_STORAGE,GHA_A,CSV archiveStyle;

        %% WARSTWA PREZENTACJI
        subgraph PRESENTATION ["📊 Insights & Visualizations"]
            GRA[Grafana Cloud <br> EMS Dashboard]
            REP[Reports & <br> ROI Analysis]
        end
        INF_S -->|Data Source: InfluxDB| GRA
        INF_G1 -->|Data Source: InfluxDB| GRA
        INF_G2 -->|Data Source: InfluxDB| GRA
        CSV -->|Data Source: Infinity Plugin| GRA
        CSV --> REP
        class PRESENTATION,GRA,REP vizStyle;
```

![EMS Dashboard if Grafana](img/grafana_dashboard_full.JPG)
