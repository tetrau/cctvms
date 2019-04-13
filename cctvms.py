import subprocess
import os
import datetime
import time
import logging

logger = logging.getLogger("cctvms")
logger.addHandler(logging.NullHandler())


class VLCRecordingError(Exception):
    def __init__(self, stdout, stderr, start, end, duration, actual_duration, record_filename):
        self.stdout = stdout
        self.stderr = stderr
        self.start = start
        self.end = end
        self.duration = duration
        self.actual_duration = actual_duration
        self.record_filename = record_filename

    def __str__(self):
        message = "VLC exited earlier than excepted\n" \
                  "start: {}\n" \
                  "end: {}\n" \
                  "actual duration: {} s\n" \
                  "excepted duration: {} s".format(datetime.datetime.fromtimestamp(self.start).isoformat(),
                                                   datetime.datetime.fromtimestamp(self.end).isoformat(),
                                                   self.actual_duration,
                                                   self.duration)
        r = "{}\n" \
            "stdout:\n" \
            "{}\n" \
            "stderr:\n" \
            "{}".format(message, self.stdout.decode(), self.stderr.decode())
        return r


class CCTVMS:
    def __init__(self, rtsp, record_dir, segment=1800,
                 prefix="record-", datetime_format="%Y-%m-%dT%H:%M", alignment=True,
                 remove_older_than=None, max_retries=3, retry_interval=10):
        """
        :param rtsp: the rtsp stream url, should be something like rtsp://xxx.xx/xxx
        :param record_dir: the direction where you store the recording files
        :param segment: duration of each record segment/file in seconds (int only)
        :param prefix: the recording filename prefix
        :param datetime_format: the recording filename timestamp format
        :param alignment: Alignment the recording segment starting time and
                          ending time to integral multiple of segment length
                          it will make the filenames more tidy
        :param remove_older_than: remove recording file ended time older than
                                  remove_older_than seconds
        :param max_retries: retry times after error happened during recording,
                            set to 0 to disable retry
        :param retry_interval: interval between each retry in seconds
        """
        self.rtsp = rtsp
        self.record_dir = os.path.abspath(record_dir)
        self.segment = int(segment)
        self.prefix = prefix
        self.datetime_format = datetime_format
        self.alignment = alignment
        self.remove_older_than = remove_older_than
        self.max_retries = max_retries
        self.retry_interval = retry_interval

    def retry(self, e):
        for i in range(self.max_retries):
            logger.error("start retry, {} time(s)".format(i + 1))
            try:
                self.record()
                return
            except VLCRecordingError as e:
                logger.error("retry failed")
                self.correct_filename(e)
                time.sleep(self.retry_interval)
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
             "--sout=file/ts:{}".format(os.path.join(self.record_dir, filename)),
             "-I", "dummy",
             "--stop-time={}".format(duration),
             "vlc://quit"],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        end = time.time()
        actual_duration = end - start
        if actual_duration < duration - 3:
            raise VLCRecordingError(stderr=p.stderr,
                                    stdout=p.stdout,
                                    start=start,
                                    end=end,
                                    actual_duration=actual_duration,
                                    duration=duration,
                                    record_filename=filename)
        logger.info("end recording")

    def output_filename(self, duration, start=None):
        now = time.time() if start is None else start
        end = now + duration
        now = datetime.datetime.fromtimestamp(now).strftime(self.datetime_format)
        end = datetime.datetime.fromtimestamp(end).strftime(self.datetime_format)
        return "{}{}_{}.ts".format(self.prefix, now, end)

    def remove_old_files(self):
        if not self.remove_older_than:
            return
        for filename in os.listdir(self.record_dir):
            if filename.startswith(self.prefix):
                filename = os.path.splitext(filename)[0]
                timestamps = filename[len(self.prefix):]
                try:
                    _, end = timestamps.split("_")
                    end = datetime.datetime.strptime(end, self.datetime_format)
                except ValueError:
                    continue
                if time.time() - end.timestamp() > self.remove_older_than:
                    file = os.path.join(self.record_dir, filename)
                    logger.info("remove old file: {}".format(file))
                    os.remove(file)

    def correct_filename(self, e: VLCRecordingError):
        filename = e.record_filename
        correct_filename = self.output_filename(duration=e.actual_duration, start=e.start)
        logger.info("correct filename from {} to {}".format(filename, correct_filename))
        os.rename(os.path.join(self.record_dir, filename),
                  os.path.join(self.record_dir, correct_filename))

    def cycle(self):
        try:
            self.record()
        except VLCRecordingError as e:
            self.correct_filename(e)
            self.retry(e)
        self.remove_old_files()

    def run(self):
        while True:
            self.cycle()
