import os

import urllib.request

# Define the path to the links_list.txt file
links_file = 'links_list.txt'

# Define the path to the data/raw_data directory
output_dir = './data/raw_data'

# Create the output directory if it doesn't exist
os.makedirs(output_dir, exist_ok=True)

# Read the URLs from the links_list.txt file
with open(links_file, 'r') as file:
    urls = file.readlines()

# Download the data from each URL and save it to the output directory
for url in urls:
    url = url.strip()  # Remove any leading/trailing whitespace or newline characters
    filename = os.path.basename(url)  # Extract the filename from the URL
    output_path = os.path.join(output_dir, filename)  # Create the output file path

    try:
        urllib.request.urlretrieve(url, output_path)
        print(f"Downloaded {filename} successfully.")
    except Exception as e:
        print(f"Failed to download {filename}: {str(e)}")