from queue import Queue, Empty
from threading import Thread

import requests
import numpy as np
import cv2
from os.path import join, isfile

from requests import Session
from requests.adapters import HTTPAdapter


def wallpaper_proc(wall_queue: Queue, session: Session):
    while True:
        wallpaper = wall_queue.get(block=False)
        print('remaining {}'.format(wall_queue.qsize()))
        try:
            filename = join('images', wallpaper + '.jpg')
            if isfile(filename):
                continue

            response = session.get('https://wallpapers.wallhaven.cc/wallpapers/full/wallhaven-{}.jpg'.format(wallpaper))
            if response.status_code == 404:
                response = session.get('https://wallpapers.wallhaven.cc/wallpapers/full/wallhaven-{}.png'.format(wallpaper))

            if response.status_code != 200:
                print('http code {} wallpaper {}'.format(response.status_code, wallpaper))
                wall_queue.put(wallpaper)
                continue

            image = np.frombuffer(response.content, dtype=np.uint8)
            image = cv2.imdecode(image, cv2.IMREAD_COLOR)
            image = cv2.resize(image, (500, 500))

            cv2.imwrite(filename, image)
        except Empty:
            break
        except Exception as e:
            wall_queue.put(wallpaper)
            print('exception {}'.format(str(e)))


if __name__ == "__main__":

    with open('metadata/wallpapers.txt') as f:
        wallpapers = f.read().splitlines()

    wall_queue = Queue()
    for wallpaper in wallpapers:
        wall_queue.put(wallpaper)

    sess = requests.Session()
    adapter = HTTPAdapter(pool_connections=100, pool_maxsize=100)
    sess.mount('https://', adapter)

    threads = []
    for i in range(75):
        thread = Thread(target=wallpaper_proc, args=(wall_queue, sess))
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()
