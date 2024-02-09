import urllib.request
import os

# Example usage
url_to_download = "https://repo1.maven.org/maven2/net/snowflake/spark-snowflake_2.13/2.14.0-spark_3.4/spark-snowflake_2.13-2.14.0-spark_3.4.jar"
downloads_folder = os.getcwd()


def download_file(url, destination_folder):
    try:
        # Create the downloads folder if it doesn't exist
        if not os.path.exists(destination_folder):
            os.makedirs(destination_folder)

        # Extract the file name from the URL
        file_name = os.path.join(destination_folder, url.split("/")[-1])

        # Download the file
        urllib.request.urlretrieve(url, file_name)

        print(f"File downloaded successfully: {file_name}")
    except Exception as e:
        print(f"Error downloading file: {e}")


download_file(url_to_download, downloads_folder)
