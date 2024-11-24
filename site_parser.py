from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from selenium.webdriver.common.action_chains import ActionChains
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By  # Import By
from InquirerPy import inquirer
from bs4 import BeautifulSoup
from video_downloader import record_stream
import time
import json
import getpass

BASE_URL = "https://playsight.com"
WEB_URL = "https://web.playsight.com"
url = (
    WEB_URL + "/facility/grand-rapids-racquet-and-fitness/home"
)  # Change link for specific team


def fetch_page(url):
    # Set up Chrome options and enable network interception
    options = Options()
    options.headless = True  # Run in headless mode
    options.add_argument("--headless")
    options.add_argument("--disable-gpu")
    options.add_argument("--disable-extensions")
    options.add_argument("--no-sandbox")

    # Enable network logging
    options.set_capability("goog:loggingPrefs", {"performance": "ALL"})

    # Initialize the driver with the updated options
    driver = webdriver.Chrome(
        service=ChromeService(ChromeDriverManager().install()), options=options
    )

    driver.get(url)
    print("\n Fetching page \n")
    time.sleep(6)  # Wait for the page to fully load (adjust as needed)

    html = driver.page_source
    logs = driver.get_log("performance")  # Capture network logs
    return driver, html, logs  # Return the driver for reuse


def parse_video_items(html):
    soup = BeautifulSoup(html, "html.parser")

    videos = []
    broadcasting_courts = soup.find_all("ps-broadcasting-court")

    for court in broadcasting_courts:
        actions_div = court.find("div", class_="actions")
        name_div = court.find("div", class_="name")
        if actions_div and name_div:
            link_tag = actions_div.find("a", href=True)
            name_tag = name_div.find("span")
            if link_tag and name_tag:
                href = link_tag["href"]
                name = name_tag.get_text(strip=True)
                videos.append((name, href))
            else:
                print("No link found in actions.")
        else:
            print("No actions div found.")

    return videos


# Toggles user to select stream to record
def select_match(videos):
    # Create a menu to select a match by its name
    selected_match_name = inquirer.select(
        message="Select a match to download the page:",
        choices=[match_name for match_name, _ in videos],
    ).execute()

    # Find the URL corresponding to the selected match name
    selected_match = next(
        (video for video in videos if video[0] == selected_match_name), None
    )
    return selected_match  # Return the full (name, href) tuple


def save_logs_to_file(logs, file_path="network_logs.txt"):
    with open(file_path, "w") as file:
        for entry in logs:
            file.write(entry["message"] + "\n")
    print(f"Logs saved to {file_path}")


def sign_in(driver):
    # Navigate to the login page
    driver.get(WEB_URL + "/auth")
    time.sleep(3)

    # Ask the user for email and password
    print("\n" + "=" * 50 + "\n")
    print("Please sign into Playsight to record a stream")
    print("\n" + "=" * 50 + "\n")

    email = input("Enter your email: ")
    password = getpass.getpass("Enter your password: ")

    # Find the input fields for the username and password
    email_field = driver.find_element(
        By.XPATH, '//input[@type="email" and @autocomplete="email"]'
    )
    password_field = driver.find_element(
        By.XPATH, '//input[@type="password" and @autocomplete="password"]'
    )

    # Enter the user's credentials
    email_field.send_keys(email)
    password_field.send_keys(password)

    # Submit the form (find the login button and click it)
    login_button = driver.find_element(
        By.XPATH, '//button[@type="submit"]'
    )  # Adjust the XPath as needed
    login_button.click()
    print("\nLogging in...\n")
    time.sleep(5)  # Wait for the login process to complete


# Extract stream link
def link_from_logs(logs):
    # Iterate through the logs to find the latest .m3u8 URL
    for entry in reversed(logs):
        try:
            # Attempt to parse the log entry's message (which may not always be valid JSON)
            log_message = json.loads(entry["message"])

            # Ensure the log message contains network information and is the right method (responseReceived)
            if (
                "message" in log_message
                and "method" in log_message["message"]
                and log_message["message"]["method"] == "Network.responseReceived"
            ):
                response = log_message["message"]["params"]["response"]

                # Check if the response URL contains '.m3u8'
                url = response.get("url", "")
                if ".m3u8" in url:
                    return url  # Return the first .m3u8 URL found and stop the loop
        except json.JSONDecodeError:
            print("Error decoding JSON for entry:", entry)
        except KeyError as e:
            print(f"Error with log entry keys: {e} - Skipping entry.")
        except Exception as e:
            print(f"Unexpected error: {e} - Skipping entry.")

    return None


def check_and_select_camera_angle(driver):
    try:

        time.sleep(5)

        # Find the camera-angles div
        camera_angles_div = driver.find_element(By.CLASS_NAME, "camera-angles")
        if camera_angles_div:
            print("Camera angles detected. Preparing options...")

            # Extract all figure tags inside the camera-angles div
            figure_tags = camera_angles_div.find_elements(By.TAG_NAME, "figure")

            # Collect names from each figure tag
            angles = []
            for figure in figure_tags:
                description_div = figure.find_element(By.CLASS_NAME, "description")
                if description_div:
                    angles.append(description_div.text.strip())

            # If descriptions are found, prompt the user to select one
            if angles:
                selected_angle = inquirer.select(
                    message="Select a camera angle:",
                    choices=angles,
                ).execute()

                # Find and click the selected angle
                for figure in figure_tags:
                    description_div = figure.find_element(By.CLASS_NAME, "description")
                    if (
                        description_div
                        and description_div.text.strip() == selected_angle
                    ):
                        ActionChains(driver).move_to_element(figure).click(
                            figure
                        ).perform()
                        print(f"Selected camera angle: {selected_angle}")
                        time.sleep(3)  # Allow the view to update
                        return True  # Successfully selected an angle
            else:
                print("No camera angle descriptions available.")
                return False
        else:
            print("No camera angles available.")
            return False
    except Exception as e:
        print(f"Error checking/selecting camera angles: {e}")
        return False


if __name__ == "__main__":
    driver, html, logs = fetch_page(url)  # Initial page load
    sign_in(driver)
    videos = parse_video_items(html)
    if not videos:
        print("No live streams available")
    else:
        # User select match to record
        selected_match = select_match(videos)
        match_name, match_url = selected_match
        print(f"Selected: {match_name}")

        # Navigate to stream
        driver.get(
            "https://web.playsight.com" + match_url
        )  # Make sure you use the full URL if 'match_url' is just a relative path

        check_and_select_camera_angle(driver)

        time.sleep(10)

        # Capture performance logs to extract .m3u8
        performance_logs = driver.get_log("performance")
        link = link_from_logs(performance_logs)

        # Promp user to name video
        output_file_name = inquirer.text(
            message="Enter the name for the output file (without extension):",
            validate=lambda text: len(text) > 0 or "File name cannot be empty.",
        ).execute()

        output_file = f"out/{output_file_name}.mp4"

        # Select recording time
        recording_time = inquirer.select(
            message="Select the recording duration:",
            choices=[
                "15 seconds",
                "30 seconds",
                "1 minute",
                "15 minutes",
                "1 hour",
                "2 hours",
                "3 hours",
                "4 hours",
                "5 hours",
            ],
        ).execute()

        record_stream(link, output_file, recording_time)

        driver.quit()  # Close the driver after operation
