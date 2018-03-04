import csv
import time
from queue import Queue, Empty
from threading import Thread

import urllib3

from WallhavenApi import WallhavenApi

username = 'klein99'
password = 'nopassword'

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def get_tags(haven: WallhavenApi, page):
    wallpapers = haven.get_images_numbers(purity_nsfw=True, page=page)
    if len(wallpapers) == 0:
        return False, None
    alltags = []
    for wallpaper in wallpapers:
        tags = haven.get_image_tags_ex(wallpaper)
        if len(tags) == 0:
            print('image {} didn\'t have any tags'.format(wallpaper))
            continue
        alltags += tags
        for tag in tags:
            tag['Image'] = wallpaper
    return True, alltags


def thread_page(stack_page: list, stack_proc: list, threadnumber):
    haven = WallhavenApi()
    haven.login(username, password)
    print('started thread', threadnumber)
    while True:
        page = stack_page.pop()
        try:
            success, tags = get_tags(haven, page)
            if not success:
                print('page {} had nothing'.format(page))
                continue
            for tag in tags:
                stack_proc.append(tag)
            print('finishing page {}'.format(page))
            print('remaining page {}'.format(len(stack_page)))
        except Empty:
            break
        except Exception as e:
            stack_page.append(page)
            print('exception' + str(e))


def thread_proc(stack_proc: list):
    with open('metadata/full.csv', 'w') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=['Image', 'Name', 'Id', 'Type'], delimiter=';')
        writer.writeheader()
        while True:
            while len(stack_proc) == 0:
                pass
            tag = stack_proc.pop()
            try:
                writer.writerow(tag)
            except Exception:
                pass


if __name__ == "__main__":

    haven = WallhavenApi()
    haven.login(username, password)

    stack_page = []
    stack_proc = []

    pagecount = haven.get_pages_count(purity_nsfw=True)

    for page in range(pagecount):
        stack_page.append(page + 1)

    threads = []
    for i in range(30):
        thread = Thread(target=thread_page, args=(stack_page, stack_proc, i + 1))
        threads.append(thread)
        thread.start()

    thread = Thread(target=thread_proc, args=(stack_proc,))
    threads.append(thread)
    thread.start()

    for thread in threads:
        thread.join()
