# match-recorder

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

## Selecting team to record 
To select the team you want to record, open the site_parser.py file and update the url variable:
```python
url = WEB_URL + "/facility/pepperdine-university/home" # <---- Change This line to team of your choice
```

Run ```python3 site_parser.py``` then follow the prompts. Playsight account required
  
