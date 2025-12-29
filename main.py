import json 


with open("data/raw/coins_data.json", "r") as f:
    raw_data = json.load(f)

# Print keys of the first coin's data for verification
print(raw_data[0].keys())
print(raw_data[0]["data"])