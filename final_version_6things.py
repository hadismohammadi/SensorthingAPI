import pandas as pd
import json
from datetime import datetime
from typing import Dict, List, Any


def excel_to_sensorthings(excel_file_path: str) -> Dict[str, Any]:
    """
    Transform Excel file with multiple sheets into SensorThings API format
    
    Args:
        excel_file_path: Path to the Excel file
        
    Returns:
        Dictionary in SensorThings API format
    """
    # Read all sheets from the Excel file
    excel_data = pd.read_excel(excel_file_path, sheet_name=None)
    
    # Initialize storage for SensorThings API entities
    things = {}
    locations = {}
    observed_properties = {}
    sensors = {}
    datastreams = {}
    observations = []
    
    # Read the LEGENDE sheet to get mapping of ID_MS to HMS and Bemerkung
    id_to_hms_mapping = {}
    if "LEGENDE" in excel_data:
        legend_df = excel_data["LEGENDE"]
        # Check if HMS column exists
        if "HMS" in legend_df.columns and "ID_MS" in legend_df.columns:
            for _, row in legend_df.iterrows():
                id_ms = str(row["ID_MS"])
                hms = row["HMS"] if not pd.isna(row["HMS"]) else "Unknown"
                bemerkung = row.get("Bemerkung", "") if not pd.isna(row.get("Bemerkung", "")) else ""
                
                # Create a dictionary to store HMS and Bemerkung information
                id_to_hms_mapping[id_ms] = {
                    "hms": hms,
                    "bemerkung": bemerkung
                }
    
    # Mapping of measurement types to their units
    unit_mapping = {
        "TEMP": {"name": "Degree Celsius", "symbol": "°C", "definition": "ucum:Cel"},
        "WINDR": {"name": "Wind Direction", "symbol": "°", "definition": "ucum:deg"},
        "GLOBAL": {"name": "Global Radiation", "symbol": "W/m²", "definition": "ucum:W/m2"},
        "NIEDER": {"name": "Precipitation", "symbol": "mm", "definition": "ucum:mm"},
        "TEMPHUMUS": {"name": "Temperature Humus", "symbol": "°C", "definition": "ucum:Cel"},
        "TEMP100": {"name": "Temperature 100cm", "symbol": "°C", "definition": "ucum:Cel"},
        "TEMP20": {"name": "Temperature 20cm", "symbol": "°C", "definition": "ucum:Cel"},
        "TEMP200": {"name": "Temperature 200cm", "symbol": "°C", "definition": "ucum:Cel"},
        "TEMP35": {"name": "Temperature 35cm", "symbol": "°C", "definition": "ucum:Cel"},
        "TEMP5": {"name": "Temperature 5cm", "symbol": "°C", "definition": "ucum:Cel"},
        "TEMP50": {"name": "Temperature 50cm", "symbol": "°C", "definition": "ucum:Cel"},
        "TDR20_1": {"name": "Soil Moisture 20cm (1)", "symbol": "%", "definition": "ucum:%"},
        "TDR20_2": {"name": "Soil Moisture 20cm (2)", "symbol": "%", "definition": "ucum:%"},
        "TDR50_1": {"name": "Soil Moisture 50cm (1)", "symbol": "%", "definition": "ucum:%"},
        "TDR50_2": {"name": "Soil Moisture 50cm (2)", "symbol": "%", "definition": "ucum:%"},
        "TDR100_1": {"name": "Soil Moisture 100cm (1)", "symbol": "%", "definition": "ucum:%"},
        "TDR100_2": {"name": "Soil Moisture 100cm (2)", "symbol": "%", "definition": "ucum:%"},
        "%nFK": {"name": "Available Field Capacity", "symbol": "%", "definition": "ucum:%"},
        "TENS20_1": {"name": "Soil Tension 20cm (1)", "symbol": "kPa", "definition": "ucum:kPa"},
        "TENS20_2": {"name": "Soil Tension 20cm (2)", "symbol": "kPa", "definition": "ucum:kPa"},
        "TENS50_1": {"name": "Soil Tension 50cm (1)", "symbol": "kPa", "definition": "ucum:kPa"},
        "TENS50_2": {"name": "Soil Tension 50cm (2)", "symbol": "kPa", "definition": "ucum:kPa"},
        "TENS100_1": {"name": "Soil Tension 100cm (1)", "symbol": "kPa", "definition": "ucum:kPa"},
        "TENS100_2": {"name": "Soil Tension 100cm (2)", "symbol": "kPa", "definition": "ucum:kPa"}
    }
    
    # ObservationType for measurements
    observation_type_measurement = "http://www.opengis.net/def/observationType/OGC-OM/2.0/OM_Measurement"
    
    # Process each sheet in the Excel file
    for sheet_name, df in excel_data.items():
        # Skip the legend sheet
        if sheet_name == "LEGENDE":
            continue
            
        # Define the category based on the sheet name
        if sheet_name == "FREIFLÄCHE":
            category = "Weather Station"
        elif "Boden" in sheet_name or "Tension" in sheet_name:
            category = "Soil Sensor"
        else:
            category = "Generic Sensor"
        
        # Get the ID column and timestamp column
        id_column = "ID_MS"
        
        # Determine the timestamp column based on sheet name
        if sheet_name == "FREIFLÄCHE":
            timestamp_column = "DATUM_ZEIT"
        elif sheet_name == "Boden+Lufttemp_Bestand C°":
            timestamp_column = "DATUMZEIT"
        else:
            timestamp_column = "DATUM"
        
        # Skip if required columns aren't in the dataframe
        if id_column not in df.columns or timestamp_column not in df.columns:
            continue
            
        # Iterate through each row in the dataframe
        for _, row in df.iterrows():
            # Get the ID_MS value
            id_ms = str(row[id_column])
            
            # Get HMS and Bemerkung information if available
            hms_info = id_to_hms_mapping.get(id_ms, {"hms": "Unknown", "bemerkung": ""})
            
            # Create a unique key for the thing based on HMS AND Bemerkung
            thing_key = f"{hms_info['hms']}_{hms_info['bemerkung']}_{id_ms}"
            
            # Create a Thing entity for this ID_MS if it doesn't exist
            if thing_key not in things:
                thing_id = f"thing_{thing_key}"
                things[thing_key] = {
                    "@iot.id": thing_id,
                    "name": f"{hms_info['hms']} - {hms_info['bemerkung']} - Station {id_ms}",
                    "description": f"{category} measurement station for {hms_info['hms']} ({hms_info['bemerkung']}) with ID: {id_ms}",
                    "properties": {
                        "id_ms": id_ms,
                        "hms": hms_info['hms'],
                        "bemerkung": hms_info['bemerkung'],
                        "category": category
                    }
                }
            
            thing_id = things[thing_key]["@iot.id"]
            
            # Create Location entity for this Thing if it doesn't exist
            if thing_id not in locations:
                location_id = f"location_{thing_id.replace('thing_', '')}"
                
                # If we have HMS information, include it in the location name
                hms_name = hms_info.get('hms', 'Unknown')
                bemerkung = hms_info.get('bemerkung', '')
                location_name = f"Location for {hms_name} ({bemerkung})" if hms_name != "Unknown" else f"Location for {thing_id}"
                
                locations[thing_id] = {
                    "@iot.id": location_id,
                    "name": location_name,
                    "description": f"Measurement station location for {hms_name} ({bemerkung})",
                    "encodingType": "application/geo+json",
                    "location": {
                        "type": "Point",
                        "coordinates": [0, 0]  # Default coordinates
                    }
                }
            
            # Get the timestamp
            timestamp = row[timestamp_column]
            if pd.isna(timestamp):
                continue
                
            # Convert timestamp to ISO format string if needed
            if isinstance(timestamp, (datetime, pd.Timestamp)):
                timestamp = timestamp.isoformat()
            
            # Process each measurement column
            for column in df.columns:
                # Skip the ID and timestamp columns and unnamed columns
                if column in [id_column, timestamp_column] or column.startswith("Unnamed"):
                    continue
                
                # Get the measurement value
                value = row[column]
                
                # Skip if the value is NaN
                if pd.isna(value):
                    continue
                
                # Extract the measurement type from the column name
                # For columns like "TEMP [C°]", extract "TEMP"
                measurement_type = column.split(" ")[0]
                
                # Create ObservedProperty if it doesn't exist
                if measurement_type not in observed_properties:
                    property_id = f"op_{measurement_type}"
                    friendly_name = unit_mapping.get(measurement_type, {}).get("name", measurement_type)
                    
                    observed_properties[measurement_type] = {
                        "@iot.id": property_id,
                        "name": friendly_name,
                        "description": f"Observed property for {friendly_name}",
                        "definition": f"http://example.org/properties/{measurement_type}"
                    }
                
                # Create Sensor if it doesn't exist
                if measurement_type not in sensors:
                    sensor_id = f"sensor_{measurement_type}"
                    friendly_name = unit_mapping.get(measurement_type, {}).get("name", measurement_type)
                    
                    sensors[measurement_type] = {
                        "@iot.id": sensor_id,
                        "name": f"{friendly_name} Sensor",
                        "description": f"Sensor for measuring {friendly_name}",
                        "encodingType": "application/pdf",
                        "metadata": f"http://example.org/sensors/{measurement_type}"
                    }
                
                # Create Datastream if it doesn't exist
                # Include HMS and Bemerkung information in the datastream key
                hms_info = id_to_hms_mapping.get(id_ms, {"hms": "Unknown", "bemerkung": ""})
                location_suffix = f"_{hms_info['hms']}_{hms_info['bemerkung']}"
                
                datastream_key = f"{thing_id}_{measurement_type}{location_suffix}"
                if datastream_key not in datastreams:
                    datastream_id = f"ds_{datastream_key}"
                    friendly_name = unit_mapping.get(measurement_type, {}).get("name", measurement_type)
                    unit_of_measurement = unit_mapping.get(measurement_type, {
                        "name": "Unknown",
                        "symbol": "",
                        "definition": "ucum:"
                    })
                    
                    datastreams[datastream_key] = {
                        "@iot.id": datastream_id,
                        "name": f"{friendly_name} Datastream for {hms_info['hms']} ({hms_info['bemerkung']})",
                        "description": f"Datastream for measuring {friendly_name} at {hms_info['hms']} ({hms_info['bemerkung']})",
                        "observationType": observation_type_measurement,
                        "unitOfMeasurement": unit_of_measurement,
                        "Sensor": sensors[measurement_type],
                        "ObservedProperty": observed_properties[measurement_type]
                    }
                
                # Create Observation
                observation = {
                    "phenomenonTime": timestamp,
                    "resultTime": timestamp,
                    "result": float(value),
                    "Datastream": {"@iot.id": datastreams[datastream_key]["@iot.id"]}
                }
                
                observations.append(observation)
    
    # Construct the nested SensorThings API structure
    result = {"Things": []}
    
    # Process each Thing
    for thing_id, thing in things.items():
        thing_with_relations = thing.copy()
        
        # Add Location to the Thing
        if thing["@iot.id"] in locations:
            thing_with_relations["Locations"] = [locations[thing["@iot.id"]]]
        
        # Find Datastreams for this Thing
        thing_datastreams = []
        for datastream_key, datastream in datastreams.items():
            if datastream_key.startswith(thing["@iot.id"]):
                datastream_with_observations = datastream.copy()
                
                # Find Observations for this Datastream
                datastream_observations = []
                for observation in observations:
                    if observation["Datastream"]["@iot.id"] == datastream["@iot.id"]:
                        # Remove the Datastream reference from the observation
                        obs_copy = observation.copy()
                        obs_copy.pop("Datastream", None)
                        datastream_observations.append(obs_copy)
                
                if datastream_observations:
                    datastream_with_observations["Observations"] = datastream_observations
                
                thing_datastreams.append(datastream_with_observations)
        
        if thing_datastreams:
            thing_with_relations["Datastreams"] = thing_datastreams
        
        result["Things"].append(thing_with_relations)
    
    return result


def main():
    # Example usage
    excel_file_path = "test_sensor_data.xlsx"
    result = excel_to_sensorthings(excel_file_path)
    
    # Count the entities for summary
    thing_count = len(result["Things"])
    datastream_count = sum(len(thing.get("Datastreams", [])) for thing in result["Things"])
    observation_count = sum(
        sum(len(datastream.get("Observations", [])) 
            for datastream in thing.get("Datastreams", []))
        for thing in result["Things"]
    )
    
    # Group Things by HMS and Bemerkung for reporting
    thing_summary = {}
    for thing in result["Things"]:
        hms = thing["properties"]["hms"]
        bemerkung = thing["properties"]["bemerkung"]
        key = f"{hms} - {bemerkung}"
        if key not in thing_summary:
            thing_summary[key] = 0
        thing_summary[key] += 1
    
    # Save to JSON file
    with open("sensorthings_output.json", "w") as f:
        json.dump(result, f, indent=2)
    
    print(f"Transformation completed:")
    print(f" - {thing_count} Things created:")
    for location, count in thing_summary.items():
        print(f"   • {location}: {count} station(s)")
    print(f" - {datastream_count} Datastreams created")
    print(f" - {observation_count} Observations created")
    print("Output saved to sensorthings_output.json")


if __name__ == "__main__":
    main()
