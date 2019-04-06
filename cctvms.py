import subprocess
import os
import datetime
import time
import logging

logger = logging.getLogger("cctvms")
logger.addHandler(logging.NullHandler())


class VLCRecordingError(Exception):
    def __init__(self, stdout, stderr, message=None):
        self.stdout = stdout
        self.stderr = stderr
        self.message = message

    def __str__(self):
        message = self.message if self.message is not None else "VLC exited earlier than excepted"
        r = "{}\n" \
            "stdout:\n" \
            "{}\n" \
            "stderr:\n" \
            "{}".format(message, self.stdout.decode(), self.stderr.decode())
        return r


class CCTVMS:
    def __init__(self, rtsp, record_dir, segment=1800,
                 prefix="record-", datetime_format="%Y-%m-%dT%H:%M", alignment=True,
                 remove_older_than=None, max_retries=3):
        self.rtsp = rtsp
        self.record_dir = os.path.abspath(record_dir)
        self.segment = int(segment)
        self.prefix = prefix
        self.datetime_format = datetime_format
        self.alignment = alignment
        self.remove_older_than = remove_older_than
        self.max_retries = max_retries

    def retry(self, e):
        for i in range(self.max_retries):
            logger.error("start retry, {} time(s)".format(i + 1))
            try:
                self.record()
                return
            except VLCRecordingError as e:
                logger.error("retry failed")
                time.sleep(10)
        logger.critical("max retries exceeded")
        raise e

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
            ["vlc", "-vvv", self.rtsp,
             "--sout=file/mp4:{}".format(os.path.join(self.record_dir, filename)),
             "-I", "dummy",
             "--stop-time={}".format(duration),
             "vlc://quit"],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        end = time.time()
        actual_duration = end - start
        if actual_duration < duration - 3:
            message = "VLC exited earlier than excepted\n" \
                      "start: {}\n" \
                      "end: {}\n" \
                      "actual duration: {} s\n" \
                      "excepted duration: {} s".format(datetime.datetime.fromtimestamp(start).isoformat(),
                                                       datetime.datetime.fromtimestamp(end).isoformat(),
                                                       actual_duration,
                                                       duration)
            raise VLCRecordingError(p.stdout, p.stderr, message)
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

    def cycle(self):
        try:
            self.record()
        except VLCRecordingError as e:
            self.retry(e)
        self.remove_old_files()

    def run(self):
        while True:
            self.cycle()
