import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

import pandas as pd
from config import INFLUX_URL, INFLUX_TOKEN, INFLUX_ORG, INFLUX_BUCKET
from influxdb_client import InfluxDBClient
from influxdb_client.client.write_api import SYNCHRONOUS


def process_silver_layer():
    client = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
    query_api = client.query_api()
    write_api = client.write_api(write_options=SYNCHRONOUS)

    # 1. POBIERANIE DANYCH (Bronze) - pobieramy z zapasem 10 min
    query = f'''
    from(bucket: "{INFLUX_BUCKET}")

        |> range(start: -10m)
        |> filter(fn: (r) => r._measurement == "power" or r._measurement == "energy")
        |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
    '''
    result = query_api.query_data_frame(query)

    # Jeśli Influx zwróci listę DataFrame-ów, połączymy je w jeden
    if isinstance(result, list):
        if len(result) > 0:
            df = pd.concat(result, ignore_index=True)
        else:
            df = pd.DataFrame()
    else:
        df = result

    if df.empty:
        print("Brak nowych danych w warstwie Bronze.")
        return
    # To jest kluczowy most między Influx (_value) a Twoim czystym kodem (value)
    if '_value' in df.columns:
        df.rename(columns={'_value': 'value'}, inplace=True)
        
    # --- PRZETWARZANIE POWER ---
    df_power = df[df['_measurement'] == 'power'].copy()
    if not df_power.empty:
        # Walidacja zakresów (Domain-aware filtering)
        # SUN2000: 0-10kW, DTSU666: -10kW do 10kW
        mask = (
                ((df_power['device'] == 'SUN2000') & (df_power['value'] >= 0) & (df_power['value'] <= 10000)) |
                ((df_power['device'] == 'DTSU666') & (df_power['value'] >= -10000) & (df_power['value'] <= 10000))
        )
        df_power = df_power[mask]

        if not df_power.empty:
            # Zmiana nazwy na Silver
            df_power['_measurement'] = 'power_clean'
            # Zapis (InfluxDB nadpisze duplikaty dzięki tym samym timestampom)
            write_api.write(bucket=INFLUX_BUCKET, record=df_power,
                            data_frame_measurement_name='power_clean',
                            data_frame_tag_columns=['device'])

    # --- PRZETWARZANIE ENERGY ---
    df_energy = df[df['_measurement'] == 'energy'].copy()
    if not df_energy.empty:
        # Walidacja: liczniki energii nie mogą być ujemne
        df_energy = df_energy[df_energy['value'] >= 0]

        if not df_energy.empty:
            df_energy['_measurement'] = 'energy_clean'
            write_api.write(bucket=INFLUX_BUCKET, record=df_energy,
                            data_frame_measurement_name='energy_clean',
                            data_frame_tag_columns=['device', 'measurement_type'])

    print("Warstwa Silver zaktualizowana pomyślnie.")
    client.close()


if __name__ == "__main__":
    process_silver_layer()
