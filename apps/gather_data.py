import os
import asyncio
import aiohttp


from pyspark.sql import SparkSession

# Define the path to the links_list.txt file
links_file = '/opt/spark/data/links_list.txt'

# Define the path to the data/raw_data directory
output_dir = '/opt/spark/data/raw_data'

# Create the output directory if it doesn't exist
os.makedirs(output_dir, exist_ok=True)



async def download_file(url, output_path):
    try:
        if os.path.exists(output_path):
            print(f"{output_path} already exists. Skipping download.")
            return
        print(f"Starting to download {output_path}.")
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                with open(output_path, 'wb') as file:
                    while True:
                        chunk = await response.content.read(1024)
                        if not chunk:
                            break
                        file.write(chunk)
        print(f"Downloaded {output_path} successfully.")
    except Exception as e:
        print(f"Failed to download {output_path}: {str(e)}")

async def main():
    spark = SparkSession.builder.getOrCreate()
    tasks = []
    with open(links_file, 'r') as file:
        urls = file.readlines()
    for url in urls:
        url = url.strip()
        filename = os.path.basename(url)
        output_path = os.path.join(output_dir, filename)
        tasks.append(download_file(url, output_path))
    await asyncio.gather(*tasks)
    spark.stop()

if __name__ == "__main__":
    asyncio.run(main())
