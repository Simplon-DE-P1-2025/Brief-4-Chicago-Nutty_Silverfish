import json
import requests
import pandas as pd

def extract_data():
    print("Extracting data...")
    req = requests.get('https://data.cityofchicago.org/resource/ijzp-q8t2.json?$limit=2')
    if req.status_code != 200:
        print(f"Failed to retrieve data: {req.status_code}")
        return
    else:
        response = req.json()
        print(response)

def main():
    extract_data()

if __name__ == '__main__':
    main()