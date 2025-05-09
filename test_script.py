import pandas as pd
import json
from excel_to_sta_customized import excel_to_sensorthings
from generate_test_excel import generate_test_excel

def main():
    # First, generate the test Excel file
    print("Generating test Excel file...")
    generate_test_excel()
    
    # Then, transform it to SensorThings API format
    print("Transforming to SensorThings API format...")
    result = excel_to_sensorthings('test_sensor_data.xlsx')
    
    # Save the result to a JSON file
    with open('sensorthings_output.json', 'w') as f:
        json.dump(result, f, indent=2)
    
    # Print some statistics about the result
    print("\nTransformation completed!")
    print(f"Number of Things: {len(result['Things'])}")
    
    total_datastreams = 0
    total_observations = 0
    
    for thing in result['Things']:
        if 'Datastreams' in thing:
            thing_datastreams = len(thing['Datastreams'])
            total_datastreams += thing_datastreams
            
            thing_observations = sum(len(ds.get('Observations', [])) for ds in thing['Datastreams'])
            total_observations += thing_observations
            
            print(f"Thing {thing['name']} has {thing_datastreams} datastreams and {thing_observations} observations")
    
    print(f"Total Datastreams: {total_datastreams}")
    print(f"Total Observations: {total_observations}")
    print("\nOutput saved to sensorthings_output.json")

if __name__ == "__main__":
    main()