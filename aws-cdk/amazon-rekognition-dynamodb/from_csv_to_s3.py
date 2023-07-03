import csv
import os
import requests
import boto3

def download_files_from_csv(csv_file, s3_bucket):
    # Create an S3 client
    s3 = boto3.client('s3')

    with open(csv_file, 'r') as file:
        reader = csv.reader(file)
        for row in reader:
            url = row[0]  # Assuming the URL is in the first column of the CSV
            url = url.replace(u'\ufeff', '')

            # Download the file
            response = requests.get(url)
            if response.status_code == 200:
                file_name = url.split('/')[-1]  # Extract the file name from the URL
                with open(file_name, 'wb') as f:
                    f.write(response.content)
                
                # Upload the file to S3
                s3.upload_file(file_name, s3_bucket, file_name)
                #print(f'Uploading file: {file_name}')
                
                # Remove the local file after uploading to S3
                os.remove(file_name)
            else:
                print(f"Failed to download file from URL: {url}")

# Usage example
csv_file = 'Image_urls.csv'
s3_bucket = 'detectlabels-rekogntion-inboundimagess3bucket406-10y4rhkn8ry8k'

download_files_from_csv(csv_file, s3_bucket)
