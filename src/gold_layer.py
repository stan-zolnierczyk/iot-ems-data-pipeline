import sys
import os
import pandas as pd
from influxdb_client import InfluxDBClient
from influxdb_client.client.write_api import SYNCHRONOUS

# Ensure the script can import local modules in CI/CD environments
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from config import INFLUX_URL, INFLUX_TOKEN, INFLUX_ORG, INFLUX_BUCKET

def process_gold_layer():
    """
    Gold Layer ETL: Aggregates Silver data into hourly business metrics.
    Ensures clock-aligned resampling (top of the hour) and maintains data lineage.
    """
    client = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
    query_api = client.query_api()
    write_api = client.write_api(write_options=SYNCHRONOUS)

    # FETCH DATA (Silver Layer)
    # Fetch last 24h to ensure there is enough data for full hourly buckets
    query = f'''
    import "influxdata/influxdb/schema"

    from(bucket: "{INFLUX_BUCKET}")

        |> range(start: -24h)
        |> filter(fn: (r) => r._measurement == "power_clean" or r._measurement == "energy_clean")
    '''
    
    raw_data = query_api.query_data_frame(query)
    df = pd.concat(raw_data, ignore_index=True) if isinstance(raw_data, list) else raw_data

    if df.empty:
        print("No Silver data found for Gold processing. Exiting.")
        client.close()
        return

    # PIVOT IN PANDAS (Instead of Flux) - creates columns from 'measurement_type' and uses '_value' as data
    df = df.pivot_table(index='_time', columns='measurement_type', values='_value')

    # Data Preparation
    df.index = pd.to_datetime(df.index, utc=True)
    df = df.sort_index()
    print(f"Columns available for aggregation: {df.columns.tolist()}")

    # --- AVERAGE POWER HOURLY ---
    power_fields = ['instantaneousPower', 'powerBalance']
    available_power = df.columns.intersection(power_fields)
    
    if not available_power.empty:
        # Define device mapping for power metrics to maintain metadata consistency
        power_device_map = {
            'instantaneousPower': 'SUN2000',
            'powerBalance': 'DTSU666'
        }
        
        # Resample to full clock hours using mean
        df_power_gold = df[available_power].resample('1H', label='left').mean()
        
        # Unpivot and re-assign metadata
        df_power_gold = df_power_gold.melt(ignore_index=False, var_name='measurement_type', value_name='value').dropna()
        df_power_gold['device'] = df_power_gold['measurement_type'].map(power_device_map).fillna('Unknown_Device')

        if not df_power_gold.empty:
            write_api.write(bucket=INFLUX_BUCKET, record=df_power_gold,
                            data_frame_measurement_name='power_hourly',
                            data_frame_tag_columns=['device', 'measurement_type'])
            print(f"Processed {len(df_power_gold)} hourly power records.")

    # --- INCREMENTED ENERGY HOURLY ---
    energy_fields = ['production', 'import', 'export', 'consumption']
    available_energy = df.columns.intersection(energy_fields)
    
    if not available_energy.empty:
        # Define source-to-target device mapping to maintain data lineage across layers
        energy_device_map = {
            'production': 'SUN2000',
            'import': 'DTSU666',
            'export': 'DTSU666',
            'consumption': 'EMS_Calculated'
        }
        
        # Resample and calculate delta (consumption within the hour)
        # Note: sorting before diff is critical for cumulative counters
        df_energy_hourly = df[available_energy].sort_index().resample('1H', label='left').last().diff()

        # Data Quality: drop initial NaN from diff and prevent negative values from resets
        df_energy_hourly = df_energy_hourly.dropna()
        df_energy_hourly[df_energy_hourly < 0] = 0

        # Unpivot and re-assign original device tags based on lineage map
        df_energy_gold = df_energy_hourly.melt(ignore_index=False, var_name='measurement_type', value_name='value')
        df_energy_gold['device'] = df_energy_gold['measurement_type'].map(energy_device_map).fillna('Unknown_Device')

        if not df_energy_gold.empty:
            write_api.write(bucket=INFLUX_BUCKET, record=df_energy_gold,
                            data_frame_measurement_name='energy_hourly',
                            data_frame_tag_columns=['device', 'measurement_type'])
            print(f"Processed {len(df_energy_gold)} hourly energy records.")

    # --- BATTERY SIMULATION (10kWh Storage) ---
    if 'export' in df.columns and 'import' in df.columns:
        print("Starting 10kWh Battery Simulation...")
        
        # Calculate 1-minute increments (energy flow in each minute)
        df_batt = df[['export', 'import']].sort_index().diff().fillna(0)
        
        capacity_wh = 10000  # 10 kWh

        # Persist all simulation counters between runs: read last known values from InfluxDB
        persistence_query = f'''
from(bucket: "{INFLUX_BUCKET}")
    |> range(start: -30d)
    |> filter(fn: (r) => r._measurement == "battery_simulation")
    |> filter(fn: (r) => r._field == "value")
    |> last()
'''
        try:
            last_batt_df = query_api.query_data_frame(persistence_query)
            if isinstance(last_batt_df, list):
                last_batt_df = pd.concat(last_batt_df, ignore_index=True) if last_batt_df else pd.DataFrame()
            if not last_batt_df.empty and 'measurement_type' in last_batt_df.columns:
                batt_last = last_batt_df.set_index('measurement_type')['_value']
                current_stored_energy = float(batt_last.get('stored_energy', 0))
                current_import_sim = float(batt_last.get('import_simulated', df['import'].iloc[0]))
                current_export_sim = float(batt_last.get('export_simulated', df['export'].iloc[0]))
            else:
                current_stored_energy = 0
                current_import_sim = df['import'].iloc[0]
                current_export_sim = df['export'].iloc[0]
        except Exception:
            current_stored_energy = 0
            current_import_sim = df['import'].iloc[0]
            current_export_sim = df['export'].iloc[0]
        print(f"Battery simulation starting with stored_energy={current_stored_energy:.1f} Wh, "
              f"import_simulated={current_import_sim:.1f} Wh, export_simulated={current_export_sim:.1f} Wh")

        stored_energy_history = []
        import_sim_history = []
        export_sim_history = []

        for _, row in df_batt.iterrows():
            m_export = row['export']
            m_import = row['import']

            # Counter reset (midnight): reset simulated counters to 0, skip increment
            if m_export < 0:
                current_export_sim = 0
                m_export = 0
            if m_import < 0:
                current_import_sim = 0
                m_import = 0

            # CHARGING: Use export to charge battery
            if m_export > 0:
                charge_amount = min(m_export, capacity_wh - current_stored_energy)
                current_stored_energy += charge_amount
                m_export -= charge_amount  # Remaining export after charging

            # DISCHARGING: Use battery to cover import
            elif m_import > 0:
                discharge_amount = min(m_import, current_stored_energy)
                current_stored_energy -= discharge_amount
                m_import -= discharge_amount  # Remaining import after battery support

            current_export_sim += m_export
            current_import_sim += m_import

            stored_energy_history.append(current_stored_energy)
            export_sim_history.append(current_export_sim)
            import_sim_history.append(current_import_sim)

        # Create results DataFrame
        df_sim = pd.DataFrame(index=df_batt.index)
        df_sim['stored_energy'] = stored_energy_history
        df_sim['export_simulated'] = export_sim_history
        df_sim['import_simulated'] = import_sim_history
        
        # Prepare for InfluxDB (unpivot)
        df_sim_gold = df_sim.melt(ignore_index=False, var_name='measurement_type', value_name='value')
        df_sim_gold['device'] = 'Battery_Simulator_10kWh'

        # Selecting final columns for write consistency
        final_sim_gold = df_sim_gold[['value', 'device', 'measurement_type']]

        write_api.write(bucket=INFLUX_BUCKET, record=final_sim_gold,
                        data_frame_measurement_name='battery_simulation',
                        data_frame_tag_columns=['device', 'measurement_type'])
        print(f"Battery simulation completed for {len(df_sim)} intervals.")


    print("Gold layer update successful.")
    client.close()

if __name__ == "__main__":
    process_gold_layer()
