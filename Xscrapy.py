import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time

driver = webdriver.Edge()

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

# Define the path to your CSV file
input_csv_path = r"C:\Users\Hari Ganesh\OneDrive\Documents\Python\twitter_scrappy\twitter_links.csv"

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


# List to store scraped data for each profile
scraped_data = []

# Loop through each URL to scrape data
for url in urls:
    print(f"Scraping data for: {url}")
    profile_info = scrape_twitter_profile(url)
    scraped_data.append(profile_info)
    time.sleep(2)  # Delay to avoid rate-limiting by Twitter

# Save the scraped data to a new CSV file
output_csv_path = r'C:\Users\Hari Ganesh\OneDrive\Documents\Python\twitter_scrappy\twitter_scrapped_data.csv'  # Update path for the output CSV file
output_df = pd.DataFrame(scraped_data)
output_df.to_csv(output_csv_path, index=False)

# Close the WebDriver
driver.quit()

print("Data scraping completed and saved to CSV.")
