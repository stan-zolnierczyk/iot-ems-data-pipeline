import sys
import os
import pandas as pd
from influxdb_client import InfluxDBClient
from influxdb_client.client.write_api import SYNCHRONOUS

# Ensure the script can import local modules when running in CI/CD environments
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from config import INFLUX_URL, INFLUX_TOKEN, INFLUX_ORG, INFLUX_BUCKET

def get_last_timestamp(client):
    """
    Queries InfluxDB for the last processed record in the Silver layer 
    to enable Incremental Loading.
    """
    query = f'''
    from(bucket: "{INFLUX_BUCKET}")

        |> range(start: -7d)
        |> filter(fn: (r) => r._measurement == "energy_clean")
        |> last()
    '''
    tables = client.query_api().query(query)
    if tables and len(tables) > 0 and len(tables[0].records) > 0:
        return tables[0].records[0].get_time()
    return None

def process_silver_layer():
    """
    ETL Process: Bronze to Silver Layer.
    Handles data validation, cleaning, and normalization for IoT energy metrics.
    """
    client = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
    query_api = client.query_api()
    write_api = client.write_api(write_options=SYNCHRONOUS)
    
    # 1. INCREMENTAL LOAD LOGIC
    last_ts = get_last_timestamp(client)

    if last_ts:
        start_range = last_ts.strftime('%Y-%m-%dT%H:%M:%SZ')
        print(f"Incremental load: fetching data since {start_range}")
    else:
        start_range = "-1h"
        print("No previous data found. Performing full load from last 1 hour.")

    # 2. FETCH DATA (Bronze Layer)
    query = f'''
    from(bucket: "{INFLUX_BUCKET}")

        |> range(start: {start_range})
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

    # 3. DATA CLEANING & PREPARATION
    df['_time'] = pd.to_datetime(df['_time'])
    df = df.sort_values('_time')
    df.set_index('_time', inplace=True)
    
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
        
        df_power.loc[~power_mask, 'value'] = pd.NA
        df_power['value'] = df_power.groupby('device')['value'].transform(
            lambda x: pd.to_numeric(x).interpolate(method='linear').ffill().bfill()
        )
        df_power.dropna(subset=['value'], inplace=True)

        if not df_power.empty:
            cols_to_drop = ['result', 'table', '_start', '_stop', '_measurement']
            final_power = df_power.drop(columns=[c for c in cols_to_drop if c in df_power.columns])
            
            write_api.write(bucket=INFLUX_BUCKET, record=final_power,
                            data_frame_measurement_name='power_clean',
                            data_frame_tag_columns=['device'])

    # --- ENERGY MEASUREMENTS PROCESSING ---
    df_energy = df[df['_measurement'] == 'energy'].copy()
    
    if not df_energy.empty:
        # Create device map BEFORE pivot to preserve metadata
        device_map = df_energy.drop_duplicates('measurement_type').set_index('measurement_type')['device'].to_dict()

        df_calc = df_energy.pivot_table(index='_time', columns='measurement_type', values='value')

        for col in ['production', 'import', 'export']:
            if col not in df_calc.columns:
                df_calc[col] = pd.NA
        
        # Incremental Load friendly: fill gaps and maintain counters
        df_calc = df_calc.sort_index().ffill().fillna(0.0)

        # Main Business Logic
        df_calc['consumption'] = df_calc['production'] + df_calc['import'] - df_calc['export']

        # Monotonicity check for counters
        for col in df_calc.columns:
            diff = df_calc[col].diff()
            df_calc.loc[diff < 0, col] = pd.NA        
        df_calc = df_calc.ffill()

        df_final_energy = df_calc.melt(ignore_index=False, var_name='measurement_type', value_name='value')
        df_final_energy['device'] = df_final_energy['measurement_type'].map(device_map).fillna('EMS_Calculated')

        if not df_final_energy.empty:
            write_api.write(bucket=INFLUX_BUCKET, record=df_final_energy,
                            data_frame_measurement_name='energy_clean',
                            data_frame_tag_columns=['device', 'measurement_type'])

    print("Silver layer update successful.")
    client.close()

if __name__ == "__main__":
    process_silver_layer()
