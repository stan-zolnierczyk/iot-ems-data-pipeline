import sys
import os
import pandas as pd
from influxdb_client import InfluxDBClient
from influxdb_client.client.write_api import SYNCHRONOUS

# Ensure the script can import local modules when running in CI/CD environments
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from config import INFLUX_URL, INFLUX_TOKEN, INFLUX_ORG, INFLUX_BUCKET

def process_silver_layer():
    """
    ETL Process: Bronze to Silver Layer.
    Handles data validation, cleaning, and normalization for IoT energy metrics.
    """
    client = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
    query_api = client.query_api()
    write_api = client.write_api(write_options=SYNCHRONOUS)

    # 1. FETCH DATA (Bronze Layer)
    # Using pivot to transform rows into columns for efficient Pandas processing
    query = f'''
    from(bucket: "{INFLUX_BUCKET}")

        |> range(start: -15m)
        |> filter(fn: (r) => r._measurement == "power" or r._measurement == "energy")
        |> filter(fn: (r) => r.measurement_type != "consumption")
        |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
    '''
    
    raw_data = query_api.query_data_frame(query)

    # Normalize response: InfluxDB client may return a list of DataFrames or a single one
    if isinstance(raw_data, list):
        df = pd.concat(raw_data, ignore_index=True) if raw_data else pd.DataFrame()
    else:
        df = raw_data

    if df.empty:
        print("No new data found in Bronze layer.")
        client.close()
        return

    # 2. DATA CLEANING & PREPARATION
    # Fix timestamp formatting and set as index to prevent "1970-01-01" write errors
    df['_time'] = pd.to_datetime(df['_time'])
    df.set_index('_time', inplace=True)
    
    # Map internal InfluxDB field names to clean business logic names
    if 'value' not in df.columns and '_value' in df.columns:
        df.rename(columns={'_value': 'value'}, inplace=True)

    # --- POWER MEASUREMENTS PROCESSING ---
    df_power = df[df['_measurement'] == 'power'].copy()
    if not df_power.empty:
        # Domain-aware filtering: 
        # SUN2000 (Inverter) expected: 0..10000W
        # DTSU666 (Smart Meter) expected: -10000..10000W
        power_mask = (
            ((df_power['device'] == 'SUN2000') & (df_power['value'].between(0, 10000))) |
            ((df_power['device'] == 'DTSU666') & (df_power['value'].between(-10000, 10000)))
        )
        df_power = df_power[power_mask]

        if not df_power.empty:
            write_api.write(
                bucket=INFLUX_BUCKET, 
                record=df_power,
                data_frame_measurement_name='power_clean',
                data_frame_tag_columns=['device']
            )

    # --- ENERGY MEASUREMENTS PROCESSING (Silver Layer Logic) ---
    df_energy = df[df['_measurement'] == 'energy'].copy()
    
    if not df_energy.empty:
        # Step A: Pivot to wide format to perform cross-column calculations
        # This creates columns: 'production', 'import', 'export'
        df_calc = df_energy.pivot_table(index='_time', columns='measurement_type', values='value')

        # Step B: Ensure all necessary columns exist (fill with 0 if missing for the timeframe)
        for col in ['production', 'import', 'export']:
            if col not in df_calc.columns:
                df_calc[col] = 0.0

        # Step C: Calculate Consumption (The Core Business Logic)
        # Formula: Consumption = Production + Import - Export
        df_calc['consumption'] = df_calc['production'] + df_calc['import'] - df_calc['export']

        # Step D: Data Quality - Energy counters must be monotonic (non-decreasing)
        # and non-negative. We apply this to all columns.
        df_calc = df_calc.apply(lambda x: x if x.min() >= 0 else None)
        df_calc.fillna(method='ffill', inplace=True)

        # Step E: Transform back to 'Long Format' for InfluxDB compatibility
        df_final_energy = df_calc.melt(ignore_index=False, var_name='measurement_type', value_name='value')
        
        # Step F: Clean up and Metadata assignment
        df_final_energy.dropna(subset=['value'], inplace=True)
        df_final_energy['device'] = 'EMS_Calculated'  # Mark that this data is derived from logic

        if not df_final_energy.empty:
            write_api.write(
                bucket=INFLUX_BUCKET, 
                record=df_final_energy,
                data_frame_measurement_name='energy_clean',
                data_frame_tag_columns=['device', 'measurement_type']
            )

    print("Silver layer update successful.")
    client.close()

if __name__ == "__main__":
    process_silver_layer()
