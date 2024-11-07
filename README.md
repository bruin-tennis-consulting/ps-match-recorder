# match-recorder

Based on the [ITF Match Recorder by awest25](https://github.com/awest25/ITF-Match-Recorder). This version utilizes two Python scripts for recording live streams off of [PlaySight](https://playsight.com/). These scripts utilize [Selenium](https://www.selenium.dev/) for browser automation, [Beautiful Soup](https://www.crummy.com/software/BeautifulSoup/) for web scraping, and [FFmpeg](https://www.ffmpeg.org/) for live-stream recording . 

## Requirements
- requests
- beautifulsoup4
- InquirerPy
- ffmpeg
- selenium 
- webdriver_manager

  
To install run: 
```bash
pip install -r requirements.txt
```

Ensure you have ffmpeg installed on your system. You can download it from [FFmpeg's official website](https://ffmpeg.org/download.html). Otherwise, you can install it using Homebrew on macOS:
```bash
brew install ffmpeg
```
## Setting up a virtual environment

**Either use an existing virtual environment or follow the steps below. **

To create a virtual environment called `venv` and install the required packages, follow these steps:

1. Open a terminal or command prompt.
2. Navigate to the project directory: `~/match-recorder`.
3. Run the following command to create a virtual environment:
  ```bash
  python -m venv venv
  ```
4. Activate the virtual environment:
  - On Windows:
    ```bash
    venv\Scripts\activate
    ```
  - On macOS and Linux:
    ```bash
    source venv/bin/activate
    ```
5. Once the virtual environment is activated, install the required packages from the `requirements.txt` file:
  ```bash
  pip install -r requirements.txt
  ```

Now you have successfully set up a virtual environment called `venv` and installed the required packages.

## Selecting team to record 
To select the team you want to record, open the site_parser.py file and update the url variable:
```python
url = WEB_URL + "/facility/pepperdine-university/home" # <---- Change This line to team of your choice
```

Run python3 site_parser.py and follow the prompts. You will be asked to enter your PlaySight credentials, select a court to record, and choose the recording duration."
