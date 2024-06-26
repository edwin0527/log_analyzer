# Log analyzer
#
# Your goal is to parse a log file and do some analysis on it. The log file contains all requests to a server within a specific timeframe.
#
# We're interested in statistics about for the following method/URL definitions:
#
# GET /api/users/{user_id}/count_pending_messages
# GET /api/users/{user_id}/get_messages
# GET /api/users/{user_id}/get_friends_progress
# GET /api/users/{user_id}/get_friends_score
# POST /api/users/{user_id}
# GET /api/users/{user_id}
#
# Where user_id is the user id of the users calling the backend.
#
# The script/program should output a small analysis of the sample. It should contain the following information for each of the URLs above:
#
# * The number of times the URL was called.
# * Mean (average) response times (connect time + service time)
# * Median response times (connect time + service time)
#
# The output should be an array of dictionary/hashes, one per URL.
#
# The log format is defined as:
# {timestamp} {source}[{process}]: at={log_level} method={http_method} path={http_path} host={http_host} fwd={client_ip} dyno={responding_dyno} connect={connection_time}ms service={processing_time}ms status={http_status} bytes={bytes_sent}
#
# Example:
# 2014-01-09T06:16:53.916977+00:00 heroku[router]: at=info method=GET path=/api/users/1538823671/count_pending_messages host=mygame.heroku.com fwd="208.54.86.162" dyno=web.11 connect=5ms service=10ms status=200 bytes=33
# 2014-01-09T06:18:53.014475+00:00 heroku[router]: at=info method=GET path=/api/users/78475839/count_pending_messages host=mygame.heroku.com fwd="208.54.86.162" dyno=web.10 connect=5ms service=20ms status=200 bytes=33
# 2014-01-09T06:20:33.142889+00:00 heroku[router]: at=info method=GET path=/api/users/847383/count_pending_messages host=mygame.heroku.com fwd="208.54.86.162" dyno=web.10 connect=25ms service=55ms status=200 bytes=33
#
# Given the above three log lines, we would expect output like:
#
# [{
#   "request_identifier": "GET /api/users/{user_id}/count_pending_messages",
#   "called": 3,
#   "response_time_mean": 40.0,
#   "response_time_median": 25.0,
# }]

import requests
import json

# Method values to track
method_values = ["count_pending_messages", "get_messages", "get_friends_progress", "get_friends_score", "POST /api/users/{user_id}", "GET /api/users/{user_id}"]

# Initialize counters and response time trackers
method_counts = {method: 0 for method in method_values}
method_response_times = {method: [] for method in method_values}

# URL of the log files
small_file_url = 'https://gist.githubusercontent.com/bss/6dbc7d4d6d2860c7ecded3d21098076a/raw/244045d24337e342e35b85ec1924bca8425fce2e/sample.small.log'
large_file_url = 'https://gist.githubusercontent.com/bss/1d7b8024451dd45feb5f17e148dacee5/raw/b02adc43edb43a44b6c9c9c34626243fd8171d4e/sample.log'

# Function to extract numerical part from a string like "9ms"
def extract_number(s):
    return int(''.join(filter(str.isdigit, s)))

# Fetch the file from the URL
r = requests.get(large_file_url)

# Process each line in the log file
for line in r.iter_lines():
    info_of_interest = line.decode('utf-8').split(" ")
    
    if len(info_of_interest) > 4:
        # Check for POST /api/users/{user_id}
        if info_of_interest[3] == "method=POST" and info_of_interest[4].startswith("path=/api/users"):
            method_name_post = "POST /api/users/{user_id}"
            method_counts[method_name_post] += 1
            try:
                connect_time = extract_number(info_of_interest[-4])
                service_time = extract_number(info_of_interest[-3])
                total_response_time = connect_time + service_time
                method_response_times[method_name_post].append(total_response_time)
            except ValueError:
                continue
        
        # Check for GET /api/users/{user_id}
        if info_of_interest[3] == "method=GET" and info_of_interest[4].startswith("path=/api/users"):
            method_name_get = "GET /api/users/{user_id}"
            method_counts[method_name_get] += 1
            try:
                connect_time = extract_number(info_of_interest[-4])
                service_time = extract_number(info_of_interest[-3])
                total_response_time = connect_time + service_time
                method_response_times[method_name_get].append(total_response_time)
            except ValueError:
                continue
        
        # Check other methods
        method = info_of_interest[4].split("/")
        if len(method) > 4:
            method_name = method[4]
            if method_name in method_values:
                method_counts[method_name] += 1
                try:
                    connect_time = extract_number(info_of_interest[-4])
                    service_time = extract_number(info_of_interest[-3])
                    total_response_time = connect_time + service_time
                    method_response_times[method_name].append(total_response_time)
                except ValueError:
                    continue

# Helper function to calculate mean
def calculate_mean(times):
    return sum(times) / len(times) if times else 0

# Helper function to calculate median
def calculate_median(times):
    n = len(times)
    if n == 0:
        return 0
    sorted_times = sorted(times)
    mid = n // 2
    if n % 2 == 0:
        return (sorted_times[mid - 1] + sorted_times[mid]) / 2
    else:
        return sorted_times[mid]

# Prepare the final output
output = []
for method in method_values:
    if method_counts[method] > 0:
        response_times = method_response_times[method]
        mean_response_time = calculate_mean(response_times)
        median_response_time = calculate_median(response_times)
        
        output.append({
            "request_identifier": method,
            "called": method_counts[method],
            "response_time_mean": mean_response_time,
            "response_time_median": median_response_time
        })

print(json.dumps(output, indent=4))