# Import required libraries
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import mysql.connector
from mysql.connector import Error
import time

# Initialize WebDriver
driver = webdriver.Edge()

# Function to create a connection to the MySQL database
def create_connection():
    """Establishes connection to the MySQL database."""     
    try:
        connection = mysql.connector.connect(
            host='127.0.0.1',  # Change as per your setup
            user='root',  # Replace with your MySQL username
            password='root',  # Replace with your MySQL password
            database='twitter_scraper'  # Replace with your database name
        )
        if connection.is_connected():
            print("Connected to MySQL database")
        return connection
    except Error as e:
        print(f"Error connecting to MySQL: {e}")
        return None

# Function to insert scraped data into the database
def insert_data(connection, profile_data):
    """Inserts the scraped data into the twitter_profiles table."""
    try:
        cursor = connection.cursor()
        insert_query = """
        INSERT INTO profiles (profile_url, bio, following_count, followers_count, location, website)
        VALUES (%s, %s, %s, %s, %s, %s)
        """
        cursor.execute(insert_query, (
            profile_data['Profile URL'],
            profile_data['Bio'],
            profile_data['Following Count'],
            profile_data['Followers Count'],
            profile_data['Location'],
            profile_data['Website']
        ))
        connection.commit()
        print(f"Data inserted for {profile_data['Profile URL']}")
    except Error as e:
        print(f"Error inserting data: {e}")
    finally:
        cursor.close()

# Function to scrape Twitter profile details
def scrape_twitter_profile(url):
    """
    Given a Twitter profile URL, this function scrapes the following details:
    - Bio
    - Following Count
    - Followers Count
    - Location
    - Website (if available)

    Parameters:
    url (str): The URL of the Twitter profile to scrape.

    Returns:
    dict: A dictionary containing the profile URL and scraped data.
    """
    driver.get(url)
    wait = WebDriverWait(driver, 10)  # Timeout after 10 seconds

    # Dictionary to store the profile data
    profile_data = {
        'Profile URL': url,
        'Bio': 'N/A',
        'Following Count': 'N/A',
        'Followers Count': 'N/A',
        'Location': 'N/A',
        'Website': 'N/A'
    }

    try:
        # Scraping the Bio
        bio_element = wait.until(EC.presence_of_element_located((By.XPATH, '//div[@data-testid="UserDescription"]')))
        profile_data['Bio'] = bio_element.text if bio_element else "N/A"

        # Scraping the Following Count
        following_count_element = driver.find_element(By.XPATH, '//a[contains(@href,"following")]/span[1]')
        profile_data['Following Count'] = following_count_element.text if following_count_element else "N/A"

        # Scraping the Followers Count
        followers_count_element = driver.find_element(By.XPATH, '//a[contains(@href,"followers")]/span[1]')
        profile_data['Followers Count'] = followers_count_element.text if followers_count_element else "N/A"

        # Scraping the Location
        location_element = driver.find_element(By.XPATH, '//span[@data-testid="UserLocation"]')
        profile_data['Location'] = location_element.text if location_element else "N/A"

        # Scraping the Website
        website_element = driver.find_element(By.XPATH, '//a[@data-testid="UserUrl"]')
        profile_data['Website'] = website_element.get_attribute("href") if website_element else "N/A"

    except Exception as e:
        print(f"An error occurred while scraping {url}: {e}")

    return profile_data

# Define the path to your input CSV file
input_csv_path = r"C:\Users\Hari Ganesh\OneDrive\Documents\Python\twitter_scrappy\twitter_links.csv"

# Load URLs from the CSV file
try:
    # Load the CSV file and print the column names for verification
    urls_df = pd.read_csv(input_csv_path)
    print("Columns in CSV:", urls_df.columns.tolist())  # Print actual column names in the CSV

    # If there is no header, load with `header=None`
    # and set column names manually if needed
    if urls_df.columns[0] != "A1":  # Adjust "A1" to your actual column name
        print("The first column name is not 'A1', renaming for convenience.")
        urls_df.columns = ['A1']  # Rename columns to match expected header
    
    # Extract URLs from the 'A1' column
    urls = urls_df['A1'].dropna().tolist()
    print("URLs loaded:", urls)

except FileNotFoundError:
    print(f"Error: File not found at {input_csv_path}")
except KeyError as e:
    print(f"Column error: {e}. Check the exact column name in the CSV.")
except Exception as e:
    print("An unexpected error occurred:", e)

# Create a database connection
connection = create_connection()

if connection:
    # Loop through each URL to scrape data
    for url in urls:
        print(f"Scraping data for: {url}")
        profile_info = scrape_twitter_profile(url)
        insert_data(connection, profile_info)
        time.sleep(2)  # Delay to avoid rate-limiting

    connection.close()
    print("Data scraping and insertion completed.")

# Close the WebDriver
driver.quit()
