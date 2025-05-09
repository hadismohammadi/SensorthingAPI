import pandas as pd 
import json
import uuid
from typing import Dict, List, Any, Optional


def excel_to_sensorthings(excel_file_path: str) -> Dict[str, Any]:
    """
    Transform Excel file with multiple sheets into SensorThings API format.
    
    Args:
        excel_file_path: Path to the Excel file
        
    Returns:
        Dictionary in SensorThings API format
    """
    # Read all sheets from Excel file
    excel_data = pd.read_excel(excel_file_path, sheet_name=None)
    
    # Initialize SensorThings data structure
    sta_data = { 
        "Things": [],
        "Locations": [],
        "ObservedProperties": [],
        "Sensors": [],
        "Datastreams": [],
        "Observations": []
    }
    
    # Process data in the required order
    # 1. Things
    if "Things" in excel_data:
        things_df = excel_data["Things"]
        for _, row in things_df.iterrows():
            thing = {
                "name": row.get("name", f"Thing_{uuid.uuid4()}"),
                "description": row.get("description", ""),
                "properties": json.loads(row.get("properties", "{}")) if isinstance(row.get("properties"), str) else row.get("properties", {})
            }
            sta_data["Things"].append(thing)
    
    # 2. Locations
    if "Locations" in excel_data:
        locations_df = excel_data["Locations"]
        for _, row in locations_df.iterrows():
            location = {
                "name": row.get("name", f"Location_{uuid.uuid4()}"),
                "description": row.get("description", ""),
                "encodingType": row.get("encodingType", "application/geo+json"),
                "location": json.loads(row.get("location", '{"type": "Point", "coordinates": [0, 0]}')) 
                            if isinstance(row.get("location"), str) else row.get("location", {"type": "Point", "coordinates": [0, 0]})
            }
            # Link to Thing if provided
            if "thing_id" in row and not pd.isna(row["thing_id"]):
                location["Thing"] = {"@iot.id": row["thing_id"]}
                
            sta_data["Locations"].append(location)
    
    # 3. ObservedProperties
    if "ObservedProperties" in excel_data:
        observed_props_df = excel_data["ObservedProperties"]
        for _, row in observed_props_df.iterrows():
            observed_property = {
                "name": row.get("name", f"ObservedProperty_{uuid.uuid4()}"),
                "description": row.get("description", ""),
                "definition": row.get("definition", "")
            }
            sta_data["ObservedProperties"].append(observed_property)
    
    # 4. Sensors
    if "Sensors" in excel_data:
        sensors_df = excel_data["Sensors"]
        for _, row in sensors_df.iterrows():
            sensor = {
                "name": row.get("name", f"Sensor_{uuid.uuid4()}"),
                "description": row.get("description", ""),
                "encodingType": row.get("encodingType", "application/pdf"),
                "metadata": row.get("metadata", "")
            }
            sta_data["Sensors"].append(sensor)
    
    # 5. Datastreams
    if "Datastreams" in excel_data:
        datastreams_df = excel_data["Datastreams"]
        for _, row in datastreams_df.iterrows():
            # Parse unitOfMeasurement
            unit_of_measurement = json.loads(row.get("unitOfMeasurement", '{"name": "", "symbol": "", "definition": ""}')) \
                                 if isinstance(row.get("unitOfMeasurement"), str) else row.get("unitOfMeasurement", {})
            
            datastream = {
                "name": row.get("name", f"Datastream_{uuid.uuid4()}"),
                "description": row.get("description", ""),
                "observationType": row.get("observationType", "http://www.opengis.net/def/observationType/OGC-OM/2.0/OM_Measurement"),
                "unitOfMeasurement": unit_of_measurement
            }
            
            # Link to Thing if provided
            if "thing_id" in row and not pd.isna(row["thing_id"]):
                datastream["Thing"] = {"@iot.id": row["thing_id"]}
                
            # Link to Sensor if provided
            if "sensor_id" in row and not pd.isna(row["sensor_id"]):
                datastream["Sensor"] = {"@iot.id": row["sensor_id"]}
                
            # Link to ObservedProperty if provided
            if "observed_property_id" in row and not pd.isna(row["observed_property_id"]):
                datastream["ObservedProperty"] = {"@iot.id": row["observed_property_id"]}
                
            sta_data["Datastreams"].append(datastream)
    
    # 6. Observations
    if "Observations" in excel_data:
        observations_df = excel_data["Observations"]
        for _, row in observations_df.iterrows():
            observation = {
                "phenomenonTime": row.get("phenomenonTime", None),
                "resultTime": row.get("resultTime", None),
                "result": row.get("result", None),
                "resultQuality": row.get("resultQuality", None),
                "parameters": json.loads(row.get("parameters", "{}")) if isinstance(row.get("parameters"), str) else row.get("parameters", {})
            }
            
            # Remove None values
            observation = {k: v for k, v in observation.items() if v is not None}
            
            # Link to Datastream if provided
            if "datastream_id" in row and not pd.isna(row["datastream_id"]):
                observation["Datastream"] = {"@iot.id": row["datastream_id"]}
                
            # Handle FeatureOfInterest if provided
            if "feature_of_interest" in row and not pd.isna(row["feature_of_interest"]):
                feature_data = json.loads(row["feature_of_interest"]) if isinstance(row["feature_of_interest"], str) else row["feature_of_interest"]
                observation["FeatureOfInterest"] = feature_data
                
            sta_data["Observations"].append(observation)
    
    # Create the final nested structure based on relationships
    final_output = construct_nested_structure(sta_data)
    
    return final_output


def construct_nested_structure(sta_data: Dict[str, List[Dict[str, Any]]]) -> Dict[str, Any]:
    """
    Construct the nested SensorThings API structure from the parsed data.
    
    Args:
        sta_data: Dictionary containing parsed data for each entity type
        
    Returns:
        Nested SensorThings API structure
    """
    result = {"Things": []}
    
    # Start with Things as the root entities
    for thing in sta_data["Things"]:
        thing_id = thing.get("@iot.id", len(result["Things"]) + 1)
        thing_with_relations = thing.copy()
        thing_with_relations["@iot.id"] = thing_id
        
        # Associate Locations with this Thing
        thing_locations = [loc for loc in sta_data["Locations"] 
                          if "Thing" in loc and loc["Thing"].get("@iot.id") == thing_id]
        if thing_locations:
            thing_with_relations["Locations"] = thing_locations
            
        # Associate Datastreams with this Thing
        thing_datastreams = [ds for ds in sta_data["Datastreams"] 
                            if "Thing" in ds and ds["Thing"].get("@iot.id") == thing_id]
        
        # For each Datastream, associate Sensor and ObservedProperty
        for ds in thing_datastreams:
            # Associate Sensor
            if "Sensor" in ds:
                sensor_id = ds["Sensor"].get("@iot.id")
                matching_sensors = [s for s in sta_data["Sensors"] if s.get("@iot.id") == sensor_id]
                if matching_sensors:
                    ds["Sensor"] = matching_sensors[0]
            
            # Associate ObservedProperty
            if "ObservedProperty" in ds:
                op_id = ds["ObservedProperty"].get("@iot.id")
                matching_ops = [op for op in sta_data["ObservedProperties"] if op.get("@iot.id") == op_id]
                if matching_ops:
                    ds["ObservedProperty"] = matching_ops[0]
            
            # Associate Observations with this Datastream
            ds_id = ds.get("@iot.id")
            ds_observations = [obs for obs in sta_data["Observations"] 
                              if "Datastream" in obs and obs["Datastream"].get("@iot.id") == ds_id]
            if ds_observations:
                ds["Observations"] = ds_observations
        
        if thing_datastreams:
            thing_with_relations["Datastreams"] = thing_datastreams
            
        result["Things"].append(thing_with_relations)
    
    return result


def main():
    # Example usage
    excel_file_path = "input.xlsx"
    result = excel_to_sensorthings(excel_file_path)
    
    # Save to JSON file
    with open("sensorthings_output.json", "w") as f:
        json.dump(result, f, indent=2)
    
    print("Transformation completed. Output saved to sensorthings_output.json")


if __name__ == "__main__":
    main()