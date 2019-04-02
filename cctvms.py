import subprocess
import os
import datetime
import time
import logging

logger = logging.getLogger("cctvms")
logger.addHandler(logging.NullHandler())


class VLCRecordingError(Exception):
    def __init__(self, stdout, stderr):
        self.stdout = stdout
        self.stderr = stderr

    def __str__(self):
        r = "VLC exited earlier than excepted\n" \
            "stdout:\n" \
            "{}\n" \
            "stderr:\n" \
            "{}".format(self.stdout.decode(), self.stderr.decode())
        return r


class CCTVMS:
    def __init__(self, rtsp, record_dir, segment=1800,
                 prefix="record-", datetime_format="%Y-%m-%dT%H:%M", alignment=True,
                 remove_older_than=None):
        self.rtsp = rtsp
        self.record_dir = os.path.abspath(record_dir)
        self.segment = int(segment)
        self.prefix = prefix
        self.datetime_format = datetime_format
        self.alignment = alignment
        self.remove_older_than = remove_older_than

    def error_handler(self, stdout, stderr):
        raise VLCRecordingError(stdout, stderr)

    def record(self):
        if self.alignment:
            now = int(time.time())
            duration = self.segment - (now % self.segment)
        else:
            duration = self.segment
        filename = self.output_filename(duration)
        logger.info("start recording rtsp stream to {}".format(filename))
        start = time.time()
        p = subprocess.run(
            ["vlc", self.rtsp,
             "--sout=file/mp4:{}".format(os.path.join(self.record_dir, filename)),
             "-I", "dummy",
             "--stop-time={}".format(duration),
             "vlc://quit"],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        actual_duration = time.time() - start
        if actual_duration < duration - 3:
            self.error_handler(p.stdout, p.stderr)
        logger.info("end recording")

    def output_filename(self, duration):
        now = time.time()
        end = now + duration
        now = datetime.datetime.fromtimestamp(now).strftime(self.datetime_format)
        end = datetime.datetime.fromtimestamp(end).strftime(self.datetime_format)
        return "{}{}_{}.mp4".format(self.prefix, now, end)

    def remove_old_files(self):
        if not self.remove_older_than:
            return
        for filename in os.listdir(self.record_dir):
            if filename.startswith(self.prefix):
                timestamps = filename[len(self.prefix):-4]
                try:
                    _, end = timestamps.split("_")
                    end = datetime.datetime.strptime(self.datetime_format, end)
                except ValueError:
                    continue
                if time.time() - end.timestamp() > self.remove_older_than:
                    os.remove(os.path.join(self.record_dir, filename))

    def run(self):
        while True:
            self.record()
            self.remove_old_files()
