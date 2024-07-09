import json
import csv
from datetime import datetime

def read_json_file(file_path):
    with open(file_path, 'r', encoding='utf-8') as file:
        data = file.readlines()
    return data

def parse_json_objects(json_lines):
    rows = []
    for line in json_lines:
        try:
            tweet = json.loads(line.strip())
            created_at_raw = tweet['interaction']['created_at']
            created_at = datetime.strptime(created_at_raw, '%a, %d %b %Y %H:%M:%S +0000').isoformat()
            content = tweet['interaction']['content']
            rows.append([created_at, content])
        except json.JSONDecodeError:
            print(f"Error decoding JSON: {line}")
        except KeyError as e:
            print(f"Missing key {e} in JSON: {line}")
        except ValueError as e:
            print(f"Error parsing date: {created_at_raw} in JSON: {line}")
    return rows

def write_to_csv(file_path, rows):
    with open(file_path, 'w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow(['created_at', 'content'])
        writer.writerows(rows)
input_file_path = 'data/dataset1.json' 
output_file_path = 'data/tweet_data.csv' 

json_lines = read_json_file(input_file_path)
rows = parse_json_objects(json_lines)
write_to_csv(output_file_path, rows)
