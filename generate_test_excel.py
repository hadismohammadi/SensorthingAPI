import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random

def generate_test_excel():
    """
    Generate a test Excel file with dummy data that matches the specified structure.
    """
    # Create a writer to save all dataframes to one Excel file
    excel_writer = pd.ExcelWriter('test_sensor_data.xlsx', engine='xlsxwriter')
    
    # Generate dummy data for each sheet
    generate_legende_sheet(excel_writer)
    generate_freiflache_sheet(excel_writer)
    generate_boden_lufttemp_sheet(excel_writer)
    generate_bodenfeu_vol_sheet(excel_writer)
    generate_bodenfeu_nfk_sheet(excel_writer)
    generate_tension_sheet(excel_writer)
    
    # Save the Excel file
    excel_writer.close()
    print("Test Excel file created: test_sensor_data.xlsx")

def generate_legende_sheet(writer):
    """Generate LEGENDE sheet"""
    # Define data
    data = {
        'ID': ['MS1', 'MS2', 'MS3'],
        'HMS': ['Hauptmessstation', 'Nebenstation', 'Feldstation'],
        'Bemerkung': ['Standort 1', 'Standort 2', 'Standort 3'],
        'Unnamed: 3': [None, None, None],
        'Unnamed: 4': [None, None, None]
    }
    
    # Create dataframe
    df = pd.DataFrame(data)
    
    # Write to Excel
    df.to_excel(writer, sheet_name='LEGENDE', index=False)

def generate_freiflache_sheet(writer):
    """Generate FREIFLÄCHE sheet"""
    # Create 20 timestamps with hourly intervals
    timestamps = [datetime(2024, 5, 1, 0, 0) + timedelta(hours=i) for i in range(20)]
    
    # Generate data for three measurement stations
    data = []
    for id_ms in ['MS1', 'MS2', 'MS3']:
        for ts in timestamps:
            # Generate some realistic-looking values with small variations
            temp_base = 20 + 5 * np.sin(ts.hour / 12 * np.pi)
            temp = round(temp_base + random.uniform(-1, 1), 1)
            
            wind = random.randint(0, 359)
            
            # Higher global radiation during daytime
            if 6 <= ts.hour <= 18:
                global_radiation = round(800 * np.sin((ts.hour - 6) / 12 * np.pi) + random.uniform(-50, 50), 1)
            else:
                global_radiation = round(random.uniform(0, 20), 1)
            
            # Occasional precipitation
            if random.random() < 0.2:  # 20% chance of precipitation
                nieder = round(random.uniform(0.1, 5.0), 1)
            else:
                nieder = 0.0
                
            data.append([id_ms, ts, temp, wind, global_radiation, nieder])
    
    # Create dataframe
    df = pd.DataFrame(data, columns=['ID_MS', 'DATUM_ZEIT', 'TEMP [C°]', 'WINDR', 'GLOBAL [W/m²]', 'NIEDER [mm]'])
    
    # Write to Excel
    df.to_excel(writer, sheet_name='FREIFLÄCHE', index=False)

def generate_boden_lufttemp_sheet(writer):
    """Generate Boden+Lufttemp_Bestand C° sheet"""
    # Create 20 timestamps with hourly intervals
    timestamps = [datetime(2024, 5, 1, 0, 0) + timedelta(hours=i) for i in range(20)]
    
    # Generate data for three measurement stations
    data = []
    for id_ms in ['MS1', 'MS2', 'MS3']:
        for ts in timestamps:
            # Generate temperature values with realistic depth gradients
            temp_humus = round(18 + 4 * np.sin(ts.hour / 12 * np.pi) + random.uniform(-1, 1), 1)
            temp_5 = round(17 + 3 * np.sin(ts.hour / 12 * np.pi) + random.uniform(-0.8, 0.8), 1)
            temp_20 = round(16 + 2 * np.sin(ts.hour / 12 * np.pi) + random.uniform(-0.6, 0.6), 1)
            temp_35 = round(15 + 1.5 * np.sin(ts.hour / 12 * np.pi) + random.uniform(-0.4, 0.4), 1)
            temp_50 = round(14 + 1 * np.sin(ts.hour / 12 * np.pi) + random.uniform(-0.3, 0.3), 1)
            temp_100 = round(12 + 0.5 * np.sin(ts.hour / 12 * np.pi) + random.uniform(-0.2, 0.2), 1)
            temp_200 = round(10 + 0.2 * np.sin(ts.hour / 12 * np.pi) + random.uniform(-0.1, 0.1), 1)
            
            data.append([id_ms, ts, temp_humus, temp_100, temp_20, temp_200, temp_35, temp_5, temp_50])
    
    # Create dataframe
    df = pd.DataFrame(data, columns=['ID_MS', 'DATUMZEIT', 'TEMPHUMUS', 'TEMP100', 'TEMP20', 'TEMP200', 'TEMP35', 'TEMP5', 'TEMP50'])
    
    # Write to Excel
    df.to_excel(writer, sheet_name='Boden+Lufttemp_Bestand C°', index=False)

def generate_bodenfeu_vol_sheet(writer):
    """Generate Bodenfeu % Vol. Wassergehalt sheet"""
    # Create 10 timestamps with daily intervals
    timestamps = [datetime(2024, 5, 1, 0, 0) + timedelta(days=i) for i in range(10)]
    
    # Generate data for three measurement stations
    data = []
    for id_ms in ['MS1', 'MS2', 'MS3']:
        for ts in timestamps:
            # Generate soil moisture values with realistic depth patterns
            # Deeper soil typically has more stable moisture
            tdr20_1 = round(20 + random.uniform(-5, 5), 1)
            tdr20_2 = round(tdr20_1 + random.uniform(-2, 2), 1)  # Similar to first sensor
            
            tdr50_1 = round(25 + random.uniform(-3, 3), 1)
            tdr50_2 = round(tdr50_1 + random.uniform(-1.5, 1.5), 1)
            
            tdr100_1 = round(30 + random.uniform(-2, 2), 1)
            tdr100_2 = round(tdr100_1 + random.uniform(-1, 1), 1)
            
            data.append([id_ms, ts, tdr20_1, tdr20_2, tdr50_1, tdr50_2, tdr100_1, tdr100_2])
    
    # Create dataframe
    df = pd.DataFrame(data, columns=['ID_MS', 'DATUM', 'TDR20_1', 'TDR20_2', 'TDR50_1', 'TDR50_2', 'TDR100_1', 'TDR100_2'])
    
    # Write to Excel
    df.to_excel(writer, sheet_name='Bodenfeu % Vol. Wassergehalt', index=False)

def generate_bodenfeu_nfk_sheet(writer):
    """Generate Bodenfeu %nFK sheet"""
    # Create 10 timestamps with daily intervals
    timestamps = [datetime(2024, 5, 1, 0, 0) + timedelta(days=i) for i in range(10)]
    
    # Generate data for three measurement stations
    data = []
    for id_ms in ['MS1', 'MS2', 'MS3']:
        for ts in timestamps:
            # Generate nFK values (typically 0-100%)
            nfk = round(60 + random.uniform(-20, 20), 1)
            data.append([id_ms, ts, nfk])
    
    # Create dataframe
    df = pd.DataFrame(data, columns=['ID_MS', 'DATUM', '%nFK'])
    
    # Write to Excel
    df.to_excel(writer, sheet_name='Bodenfeu %nFK', index=False)

def generate_tension_sheet(writer):
    """Generate Tension kPa sheet"""
    # Create 10 timestamps with daily intervals
    timestamps = [datetime(2024, 5, 1, 0, 0) + timedelta(days=i) for i in range(10)]
    
    # Generate data for three measurement stations
    data = []
    for id_ms in ['MS1', 'MS2', 'MS3']:
        for ts in timestamps:
            # Generate soil tension values (kPa)
            # Lower values indicate wetter soil
            tens20_1 = round(15 + random.uniform(-10, 20), 1)
            tens20_2 = round(tens20_1 + random.uniform(-5, 5), 1)
            
            tens50_1 = round(25 + random.uniform(-10, 15), 1)
            tens50_2 = round(tens50_1 + random.uniform(-5, 5), 1)
            
            tens100_1 = round(30 + random.uniform(-5, 10), 1)
            tens100_2 = round(tens100_1 + random.uniform(-3, 3), 1)
            
            data.append([id_ms, ts, tens20_1, tens20_2, tens50_1, tens50_2, tens100_1, tens100_2])
    
    # Create dataframe
    df = pd.DataFrame(data, columns=['ID_MS', 'DATUM', 'TENS20_1', 'TENS20_2', 'TENS50_1', 'TENS50_2', 'TENS100_1', 'TENS100_2'])
    
    # Write to Excel
    df.to_excel(writer, sheet_name='Tension kPa', index=False)

if __name__ == "__main__":
    generate_test_excel()