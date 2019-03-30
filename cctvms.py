import subprocess
import os
import datetime
import time
import logging

logger = logging.getLogger("cctvms")
logger.addHandler(logging.NullHandler())


class CCTVMS:
    def __init__(self, rtsp, record_dir, segment=1800, prefix="record-", datetime_format="%Y-%m-%dT%H:%M"):
        self.rtsp = rtsp
        self.record_dir = os.path.abspath(record_dir)
        self.segment = segment
        self.prefix = prefix
        self.datetime_format = datetime_format

    def record(self):
        subprocess.run(
            ["vlc", self.rtsp,
             "--sout=file/mp4:{}".format(os.path.join(self.record_dir, self.output_filename())),
             "-I", "dummy",
             "--stop-time={}".format(int(self.segment)),
             "vlc://quit"],
            stdout=subprocess.DEVNULL)

    def output_filename(self):
        now = time.time()
        end = now + self.segment
        now = datetime.datetime.fromtimestamp(now).strftime(self.datetime_format)
        end = datetime.datetime.fromtimestamp(end).strftime(self.datetime_format)
        return "{}{}_{}.mp4".format(self.prefix, now, end)

    def run(self):
        while True:
            logger.info("start recording")
            self.record()
            logging.info("end recording")
