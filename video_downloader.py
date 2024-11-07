import subprocess
import sys
from InquirerPy import inquirer
from datetime import timedelta


def record_stream(url, output, duration):
    """
    Records a livestream from the given URL and saves it to the specified output file.

    Parameters:
    - stream_url (str): The URL of the livestream.
    - output_file (str): The path to the output file where the recording will be saved.
    - duration (str): The duration of the recording.
    """
    # Convert the duration to seconds
    time_mapping = {
        "15 seconds": 15,
        "30 seconds": 30,
        "1 minute": 60,
        "15 minutes": 15 * 60,
        "1 hour": 60 * 60,
        "2 hours": 2 * 60 * 60,
        "3 hours": 3 * 60 * 60
    }

    # Get the duration in seconds
    duration_seconds = time_mapping.get(duration, 15)  # Default to 15 seconds if not found

    # Convert seconds to HH:MM:SS format for ffmpeg
    time_str = str(timedelta(seconds=duration_seconds))

    # Command to record the livestream using ffmpeg
    command = [
        "ffmpeg",
        "-i",
        url,  # Input URL
        "-c",
        "copy",  # Copy the codec (do not re-encode)
        "-t",
        time_str,  # Duration to record
        output,
    ]
    try:
        # Execute the ffmpeg command
        subprocess.run(command, check=True)
        print(f"Recording saved to {output}")
    except subprocess.CalledProcessError as e:
        print(f"Error recording livestream: {e}", file=sys.stderr)



# Example usage
if __name__ == "__main__":
    # Example usage
    stream_url = "https://cdn-psnginx-usa-losangeles-01.playsight.com/hls/occidentalcollegeADMINCourt102.m3u8"
    output_file = "out/livestream_recording.mp4"

    record_stream(stream_url, output_file)
