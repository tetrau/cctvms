import subprocess
import os
import datetime
import time
import logging

logger = logging.getLogger("cctvms")
logger.addHandler(logging.NullHandler())


class CCTVMS:
    def __init__(self, rtsp, record_dir, segment=1800,
                 prefix="record-", datetime_format="%Y-%m-%dT%H:%M", alignment=True):
        self.rtsp = rtsp
        self.record_dir = os.path.abspath(record_dir)
        self.segment = int(segment)
        self.prefix = prefix
        self.datetime_format = datetime_format
        self.alignment = alignment

    def record(self):
        if self.alignment:
            now = int(time.time())
            duration = self.segment - (now % self.segment)
        else:
            duration = self.segment
        filename = self.output_filename(duration)
        logger.info("start recording rtsp stream to {}".format(filename))
        p = subprocess.run(
            ["vlc", self.rtsp,
             "--sout=file/mp4:{}".format(os.path.join(self.record_dir, filename)),
             "-I", "dummy",
             "--stop-time={}".format(duration),
             "vlc://quit"],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        logger.info("end recording")

    def output_filename(self, duration):
        now = time.time()
        end = now + duration
        now = datetime.datetime.fromtimestamp(now).strftime(self.datetime_format)
        end = datetime.datetime.fromtimestamp(end).strftime(self.datetime_format)
        return "{}{}_{}.mp4".format(self.prefix, now, end)

    def run(self):
        while True:
            self.record()
