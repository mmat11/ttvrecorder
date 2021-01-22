import argparse
import logging
import os
import threading
import time
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
    CHUNKSIZE = 2048

    class State(Enum):
        INITIALIZING = 0
        RUNNING = 1
        STOPPED = 2

    def __init__(self, channel: str, output_folder: str, quality: str):
        self.channel = channel
        self.quality = quality

        self.session = Streamlink()
        self.stopper = threading.Event()

        self.state = Recorder.State.INITIALIZING

        self.prepare_output_folder(output_folder)

    def start(self):
        assert self.state in {Recorder.State.INITIALIZING, Recorder.State.STOPPED}

        self.state = Recorder.State.RUNNING

        fd = self.session.streams(f"https://twitch.tv/{self.channel}")[
            self.quality
        ].open()

        with open(self.fullpath, "wb") as f:
            while not self.stopper.is_set():
                data = fd.read(Recorder.CHUNKSIZE)
                f.write(data)

        self.state = Recorder.State.STOPPED

    def stop(self, th: threading.Thread):
        assert self.state == Recorder.State.RUNNING

        self.stopper.set()

        while not self.state == Recorder.State.STOPPED:
            time.sleep(1)

        th.join()

        self.stopper = threading.Event()

    @property
    def filename(self) -> str:
        dt = datetime.now().strftime("%d-%m-%Y_%H-%M")
        return f"{self.channel}_{dt}.mp4"

    @property
    def fullpath(self) -> str:
        return os.path.join(self.output_folder, self.filename)

    def prepare_output_folder(self, output_folder):
        if not os.path.exists(output_folder):
            os.mkdir(output_folder)
        fp = os.path.join(output_folder, self.channel)
        if not os.path.exists(fp):
            os.mkdir(fp)

        self.output_folder = fp


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
        def loop():
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

        try:
            loop()
        except KeyboardInterrupt:
            logger.info("shutting down...")
            if self.recorder.state == Recorder.State.RUNNING:
                self.recorder.stop(self.worker)


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

    manager.run()