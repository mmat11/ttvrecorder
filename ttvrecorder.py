import argparse
import logging
import os
import subprocess
import threading
import time
from collections import deque
from dataclasses import dataclass
from datetime import datetime
from enum import Enum

import requests
from streamlink import Streamlink

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(module)s - %(funcName)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("ttv-recorder")


@dataclass
class TTVConfig:
    client_id: str
    client_secret: str

    def __post_init__(self):
        assert self.client_id
        assert self.client_secret


class Recorder:
    CHUNKSIZE = 8192

    class State(Enum):
        INITIALIZING = 0
        RUNNING = 1
        STOPPED = 2

    def __init__(self, channel: str, output_folder: str, quality: str):
        self.channel = channel
        self.quality = quality

        self.session = Streamlink()
        self.session.set_plugin_option("twitch", "twitch-disable-hosting", True)
        self.session.set_plugin_option("twitch", "twitch-disable-ads", True)
        self.session.set_plugin_option("twitch", "twitch-disable-reruns", True)
        self.session.set_plugin_option("twitch", "twitch-low-latency", True)

        self.q = deque([])
        self.stopper = threading.Event()

        self.state = Recorder.State.INITIALIZING

        self.output_folder = None
        self.current_filename = None
        self.prepare_output_folder(output_folder)

    def start(self):
        assert self.state in {Recorder.State.INITIALIZING, Recorder.State.STOPPED}

        self.state = Recorder.State.RUNNING
        self.get_new_filename()

        fd = self.session.streams(f"https://twitch.tv/{self.channel}")[
            self.quality
        ].open()

        def _writer(self):
            with open(self.current_filename, "wb") as f:
                while self.state == Recorder.State.RUNNING:
                    if self.q:
                        data = self.q.popleft()
                        f.write(data)

        file_writer = threading.Thread(target=_writer, args=(self,))
        file_writer.start()

        while not self.stopper.is_set():
            self.q.append(fd.read(Recorder.CHUNKSIZE))

        self.state = Recorder.State.STOPPED

    def stop(self, th: threading.Thread):
        assert self.state == Recorder.State.RUNNING

        self.stopper.set()

        while not self.state == Recorder.State.STOPPED:
            time.sleep(1)

        th.join()

        subprocess.run(
            [
                "/usr/bin/ffmpeg",
                "-i",
                self.current_filename,
                "-vcodec",
                "copy",
                "-acodec",
                "copy",
                self.current_filename.replace(".tmp", ".mp4"),
            ]
        )
        os.remove(self.current_filename)

        self.stopper = threading.Event()

    def get_new_filename(self):
        dt = datetime.now().strftime("%d-%m-%Y_%H-%M")
        filename = f"{self.channel}_{dt}.tmp"
        self.current_filename = os.path.join(self.output_folder, filename)

    def prepare_output_folder(self, output_folder):
        if not os.path.exists(output_folder):
            os.mkdir(output_folder)
        self.output_folder = output_folder


class Manager:
    POLL_INTERVAL = 30

    def __init__(self, recorder: Recorder, ttv_config: TTVConfig):
        self.recorder = recorder
        self.ttv_config = ttv_config

        self.token = None
        self.worker = None

    @property
    def channel(self) -> str:
        return self.recorder.channel

    def auth(self):
        logger.info("renewing token")
        resp = requests.post(
            f"https://id.twitch.tv/oauth2/token?client_id={self.ttv_config.client_id}&client_secret={self.ttv_config.client_secret}&grant_type=client_credentials"
        )
        self.token = resp.json()["access_token"]

    def is_channel_live(self) -> bool:
        resp = requests.get(
            f"https://api.twitch.tv/helix/search/channels?query={self.channel}",
            headers={
                "Client-ID": self.ttv_config.client_id,
                "Authorization": f"Bearer {self.token}",
            },
        )

        if resp.status_code == 401:
            self.auth()
            return self.is_channel_live()

        for channel in resp.json()["data"]:
            if channel["display_name"].lower() == self.channel:
                return channel["is_live"]

        raise ChannelNotFound()

    def run(self):
        while True:
            is_live = self.is_channel_live()

            if self.recorder.state in {
                Recorder.State.INITIALIZING,
                Recorder.State.STOPPED,
            }:
                if is_live:
                    # the Recorder is not running, start worker thread
                    logger.info("starting worker thread")
                    self.worker = threading.Thread(target=self.recorder.start)
                    self.worker.start()
                else:
                    # do nothing, wait for channel to go live before initializing the Recorder
                    logger.debug("channel is off, waiting...")
            else:
                assert self.recorder.state == Recorder.State.RUNNING
                if is_live:
                    # do nothing, continue recording
                    logger.debug("still recording...")
                else:
                    # stop the recording
                    logger.info("channel went off, stopping the recorder")
                    self.recorder.stop(self.worker)

            time.sleep(Manager.POLL_INTERVAL)


class ChannelNotFound(Exception):
    pass


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--channel", type=str, help="channel name")
    parser.add_argument("-o", "--output-folder", type=str, help="output folder")
    parser.add_argument(
        "-q",
        "--quality",
        type=str,
        help="recording quality, default=best",
        default="best",
    )
    parser.add_argument(
        "-tci",
        "--ttv-client-id",
        type=str,
        help="twitch client id",
        default=os.environ.get("TTV_CLIENT_ID"),
    )
    parser.add_argument(
        "-tcs",
        "--ttv-client-secret",
        type=str,
        help="twitch client secret",
        default=os.environ.get("TTV_CLIENT_SECRET"),
    )
    args = parser.parse_args()

    ttv_config = TTVConfig(
        client_id=args.ttv_client_id, client_secret=args.ttv_client_secret
    )
    rec = Recorder(
        channel=args.channel, output_folder=args.output_folder, quality=args.quality
    )
    manager = Manager(recorder=rec, ttv_config=ttv_config)

    try:
        manager.run()
    except KeyboardInterrupt:
        logger.info("shutting down...")
        if manager.recorder.state == Recorder.State.RUNNING:
            manager.recorder.stop(manager.worker)
