import pandas as pd
import json
import uuid
from typing import Dict, List, Any, Optional
from enum import Enum, auto
from datetime import datetime


class EntityType(Enum):
    """Enum for SensorThings API entity types"""
    THING = auto()
    LOCATION = auto()
    OBSERVED_PROPERTY = auto()
    SENSOR = auto()
    DATASTREAM = auto()
    OBSERVATION = auto()


class ObservationType(Enum):
    """Enum for observation types in SensorThings API"""
    MEASUREMENT = "http://www.opengis.net/def/observationType/OGC-OM/2.0/OM_Measurement"
    CATEGORY = "http://www.opengis.net/def/observationType/OGC-OM/2.0/OM_CategoryObservation"
    COUNT = "http://www.opengis.net/def/observationType/OGC-OM/2.0/OM_CountObservation"
    TRUTH = "http://www.opengis.net/def/observationType/OGC-OM/2.0/OM_TruthObservation"


class SensorThingsTransformer:
    """
    Transforms specialized Excel data into SensorThings API format.
    """
    
    def __init__(self, excel_file_path: str):
        """
        Initialize the transformer with an Excel file
        
        Args:
            excel_file_path: Path to the Excel file
        """
        self.excel_file_path = excel_file_path
        self.excel_data = None
        
        # Will hold the transformed data
        self.things = {}
        self.locations = {}
        self.observed_properties = {}
        self.sensors = {}
        self.datastreams = {}
        self.observations = []
        
        # Mapping of measurement types to their units
        self.unit_mapping = {
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
        
        # Mapping of sheet names to their description
        self.sheet_descriptions = {
            "LEGENDE": "Legend information",
            "FREIFLÄCHE": "Open area measurements including temperature, wind, radiation, and precipitation",
            "Boden+Lufttemp_Bestand C°": "Soil and air temperature measurements at different depths",
            "Bodenfeu % Vol. Wassergehalt": "Soil moisture measurements at different depths (volume water content)",
            "Bodenfeu %nFK": "Available field capacity measurements (soil moisture)",
            "Tension kPa": "Soil tension measurements at different depths"
        }
    
    def read_excel(self):
        """Read all sheets from the Excel file"""
        self.excel_data = pd.read_excel(self.excel_file_path, sheet_name=None)
    
    def create_thing(self, id_ms: str, category: str) -> Dict[str, Any]:
        """
        Create or retrieve a Thing entity based on the ID_MS
        
        Args:
            id_ms: The ID_MS value from the Excel data
            category: The measurement category (e.g., "Weather Station", "Soil Sensor")
            
        Returns:
            The Thing entity
        """
        if id_ms not in self.things:
            thing_id = f"thing_{id_ms}"
            self.things[id_ms] = {
                "@iot.id": thing_id,
                "name": f"Measurement Station {id_ms}",
                "description": f"{category} measurement station with ID {id_ms}",
                "properties": {
                    "id_ms": id_ms,
                    "category": category
                }
            }
        
        return self.things[id_ms]
    
    def create_location(self, thing_id: str) -> Dict[str, Any]:
        """
        Create a Location entity for a Thing
        
        Args:
            thing_id: The Thing ID
            
        Returns:
            The Location entity
        """
        if thing_id not in self.locations:
            location_id = f"location_{thing_id.split('_')[1]}"
            self.locations[thing_id] = {
                "@iot.id": location_id,
                "name": f"Location for {thing_id}",
                "description": "Measurement station location",
                "encodingType": "application/geo+json",
                "location": {
                    "type": "Point",
                    "coordinates": [0, 0]  # Default coordinates, would need to be updated with actual values
                }
            }
        
        return self.locations[thing_id]
    
    def create_observed_property(self, property_name: str) -> Dict[str, Any]:
        """
        Create or retrieve an ObservedProperty entity
        
        Args:
            property_name: The name of the property being observed
            
        Returns:
            The ObservedProperty entity
        """
        if property_name not in self.observed_properties:
            property_id = f"op_{property_name}"
            
            # Get friendly name and description
            friendly_name = self.unit_mapping.get(property_name, {}).get("name", property_name)
            
            self.observed_properties[property_name] = {
                "@iot.id": property_id,
                "name": friendly_name,
                "description": f"Observed property for {friendly_name}",
                "definition": f"http://example.org/properties/{property_name}"
            }
        
        return self.observed_properties[property_name]
    
    def create_sensor(self, property_name: str) -> Dict[str, Any]:
        """
        Create or retrieve a Sensor entity
        
        Args:
            property_name: The name of the property being measured
            
        Returns:
            The Sensor entity
        """
        if property_name not in self.sensors:
            sensor_id = f"sensor_{property_name}"
            
            # Get friendly name
            friendly_name = self.unit_mapping.get(property_name, {}).get("name", property_name)
            
            self.sensors[property_name] = {
                "@iot.id": sensor_id,
                "name": f"{friendly_name} Sensor",
                "description": f"Sensor for measuring {friendly_name}",
                "encodingType": "application/pdf",
                "metadata": f"http://example.org/sensors/{property_name}"
            }
        
        return self.sensors[property_name]
    
    def create_datastream(self, thing_id: str, property_name: str) -> Dict[str, Any]:
        """
        Create or retrieve a Datastream entity
        
        Args:
            thing_id: The Thing ID
            property_name: The name of the property being measured
            
        Returns:
            The Datastream entity
        """
        datastream_key = f"{thing_id}_{property_name}"
        
        if datastream_key not in self.datastreams:
            datastream_id = f"ds_{datastream_key}"
            
            # Get friendly name and unit of measurement
            friendly_name = self.unit_mapping.get(property_name, {}).get("name", property_name)
            unit_of_measurement = self.unit_mapping.get(property_name, {
                "name": "Unknown",
                "symbol": "",
                "definition": "ucum:"
            })
            
            # Create references to related entities
            sensor = self.create_sensor(property_name)
            observed_property = self.create_observed_property(property_name)
            
            self.datastreams[datastream_key] = {
                "@iot.id": datastream_id,
                "name": f"{friendly_name} Datastream for Thing {thing_id}",
                "description": f"Datastream for measuring {friendly_name}",
                "observationType": ObservationType.MEASUREMENT.value,
                "unitOfMeasurement": unit_of_measurement,
                "Sensor": sensor,
                "ObservedProperty": observed_property
            }
        
        return self.datastreams[datastream_key]
    
    def create_observation(self, datastream_id: str, timestamp, value) -> Dict[str, Any]:
        """
        Create an Observation entity
        
        Args:
            datastream_id: The Datastream ID
            timestamp: The timestamp of the observation
            value: The observed value
            
        Returns:
            The Observation entity
        """
        observation = {
            "phenomenonTime": timestamp,
            "resultTime": timestamp,
            "result": value
        }
        
        self.observations.append(observation)
        
        return observation
    
    def process_sheet(self, sheet_name: str):
        """
        Process a single sheet from the Excel file
        
        Args:
            sheet_name: The name of the sheet to process
        """
        if sheet_name == "LEGENDE":
            # Skip the legend sheet, it doesn't contain measurements
            return
            
        # Get the dataframe for this sheet
        df = self.excel_data[sheet_name]
        
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
        
        # Iterate through each row in the dataframe
        for _, row in df.iterrows():
            # Get the ID_MS value
            id_ms = str(row[id_column])
            
            # Create a Thing entity for this ID_MS
            thing = self.create_thing(id_ms, category)
            thing_id = thing["@iot.id"]
            
            # Create a Location entity for this Thing
            self.create_location(thing_id)
            
            # Get the timestamp
            timestamp = row[timestamp_column]
            if pd.isna(timestamp):
                continue
                
            # Convert timestamp to ISO format string if needed
            if isinstance(timestamp, (datetime, pd.Timestamp)):
                timestamp = timestamp.isoformat()
            
            # Process each measurement column
            for column in df.columns:
                # Skip the ID and timestamp columns
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
                
                # Create a Datastream for this measurement type
                datastream = self.create_datastream(thing_id, measurement_type)
                datastream_id = datastream["@iot.id"]
                
                # Create an Observation
                self.create_observation(datastream_id, timestamp, float(value))
    
    def construct_nested_structure(self) -> Dict[str, Any]:
        """
        Construct the nested SensorThings API structure
        
        Returns:
            The nested SensorThings API structure
        """
        # Create the result structure
        result = {"Things": []}
        
        # Process each Thing
        for thing_id, thing in self.things.items():
            thing_with_relations = thing.copy()
            
            # Add Location to the Thing
            if thing["@iot.id"] in self.locations:
                thing_with_relations["Locations"] = [self.locations[thing["@iot.id"]]]
            
            # Find Datastreams for this Thing
            thing_datastreams = []
            for datastream_key, datastream in self.datastreams.items():
                if datastream_key.startswith(thing["@iot.id"]):
                    datastream_with_observations = datastream.copy()
                    
                    # Find Observations for this Datastream
                    datastream_observations = []
                    for observation in self.observations:
                        datastream_observations.append(observation)
                    
                    if datastream_observations:
                        datastream_with_observations["Observations"] = datastream_observations
                    
                    thing_datastreams.append(datastream_with_observations)
            
            if thing_datastreams:
                thing_with_relations["Datastreams"] = thing_datastreams
            
            result["Things"].append(thing_with_relations)
        
        return result
    
    def transform(self) -> Dict[str, Any]:
        """
        Transform the Excel data into SensorThings API format
        
        Returns:
            The transformed data in SensorThings API format
        """
        # Read the Excel data
        self.read_excel()
        
        # Process each sheet
        for sheet_name in self.excel_data.keys():
            self.process_sheet(sheet_name)
        
        # Construct the nested structure
        result = self.construct_nested_structure()
        
        return result


def excel_to_sensorthings(excel_file_path: str) -> Dict[str, Any]:
    """
    Transform Excel file with multiple sheets into SensorThings API format
    
    Args:
        excel_file_path: Path to the Excel file
        
    Returns:
        Dictionary in SensorThings API format
    """
    transformer = SensorThingsTransformer(excel_file_path)
    return transformer.transform()


def main():
    # Example usage
    excel_file_path = "test_sensor_data.xlsx"
    result = excel_to_sensorthings(excel_file_path)
    
    # Save to JSON file
    with open("sensorthings_output.json", "w") as f:
        json.dump(result, f, indent=2)
    
    print("Transformation completed. Output saved to sensorthings_output.json")


if __name__ == "__main__":
    main()