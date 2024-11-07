import subprocess
import sys


def record_stream(url, output):
    """
    Records a livestream from the given URL and saves it to the specified output file.

    Parameters:
    - stream_url (str): The URL of the livestream.
    - output_file (str): The path to the output file where the recording will be saved.
    """
    # Command to record the livestream using ffmpeg
    command = [
        "ffmpeg",
        "-i",
        url,  # Input URL
        "-c",
        "copy",  # Copy the codec (do not re-encode)
        "-t",
        "00:00:15",  # Duration to record (e.g., 3 hours in this example)
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
