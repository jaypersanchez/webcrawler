#!/usr/bin/env python3

# global imports
from pymongo import MongoClient
from bs4 import BeautifulSoup
from dateutil.tz import tzlocal
import traceback
import requests
import argparse
import datetime
import os.path
import fcntl  # for file-locking
import errno
import time
import re
import os

DB_HOST = "localhost"
DB_PORT = 27017
DB_NAME = "social"
DB_COLLECTION = "urllist"

# timeout for web page requests (in seconds)
REQUEST_TIMEOUT = 30


"""
File locking (to prevent simultaneous reading and writing to the same file)

This is necessary because if two instances of the script are accidentally run,
 they might crawl duplicate data because one might be writing to the file at the same
 time the other is reading.
"""


def wait_and_lock_file(file_handle):
    """
    Waits for the file to be unlocked, then acquire the lock
    """
    while True:
        try:
            fcntl.flock(file_handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
            break
        except IOError as e:
            # raise on unrelated IOErrors
            if e.errno != errno.EAGAIN:
                raise
            else:
                print("waiting for file '{}' to be unlocked (it's already being used by another instance of this script)".format(file_handle.name))
                time.sleep(1)


def unlock_file(file_handle):
    """
    Release the lock
    """
    fcntl.flock(file_handle, fcntl.LOCK_UN)


"""
"""


def parse_args():
    """
    Parse the arguments from the command line
    """
    parser = argparse.ArgumentParser(description="Crawler for sentiment analysis")
    parser.add_argument("directory", help=("firectory to save the file"))
    parser.add_argument("filename", help=("file to write the output lines"))
    parser.add_argument("--filter", dest="filter", help="filters for URLs. The script will only crawl from URLs containing these filters. If there's more than 1 filter, use a comma to separate them (ex: --filter 'politics, finance, ...')")
    parser.add_argument("--host", dest="host", help="mongodb host")
    parser.add_argument("--port", dest="port", help="mongodb port")
    parser.add_argument("--database", dest="database", help="mongodb database name to be used")
    parser.add_argument("--collection", dest="collection", help="mongodb collection to be used")
    args = vars(parser.parse_args())
    return args


def get_urls_from_mongodb(host, port, database, collection):
    client = MongoClient(host, port)
    db = client[database]
    col = db[collection]

    documents = col.find()

    urls = set()

    for document in documents:
        url_list = document["url"]
        for url in url_list:
            urls.add(url)

    return urls


def urls_crawled_in_file(file_path):
    """
    Read existing lines and get the urls already saved to avoid duplicates.
    """
    lines = []

    urls_crawled = set()

    try:
        if os.path.isfile(file_path):
            with open(file_path, "r") as file_input:
                lines = file_input.readlines()
            lines = [x.strip() for x in lines]  # remove whitespace and newline characters

        for line in lines:
            values = line.split("^")
            if len(values) != 8:
                raise Exception("Invalid line format")
            twitter_id = values[4]
            urls_crawled.add(twitter_id)
    except Exception as e:
        pass

    return urls_crawled


def urls_crawled_in_dir(dir_path):
    files = [f for f in os.listdir(dir_path) if os.path.isfile(os.path.join(dir_path, f))]

    urls_crawled = set()
    for file in files:
        urls_crawled.update(urls_crawled_in_file(os.path.join(dir_path, file)))

    return urls_crawled


def current_date_for_timestamp():
    """
    Get the current date/time with the timezone.
    Used in the 'dateOfTweet' field.
    """
    now = datetime.datetime.now(tzlocal())
    return now.strftime('%a %b %d %H:%M:%S %Z %Y')


def save_result(file_handle, result):
    """
    Write a result (dictionary with the results from a story) to the file.
    """
    location_long = "locationLong"
    location_lat = "locationLat"
    verbose_location = "spider"
    user_name = "userName"
    screen_name = "screenName"
    date_of_tweet = current_date_for_timestamp()

    twitter_id = result["url"]

    tweeted_text = "<title>{}</title><body>{}</body>".format(result["title"], result["body"])

    file_handle.write("{}^{}^{}^{}^{}^{}^{}^{}\n".format(
                        location_long,
                        location_lat,
                        verbose_location,
                        date_of_tweet,
                        twitter_id,
                        user_name,
                        screen_name,
                        tweeted_text
                        ))


def crawl(url):
    """
    Extract the data (text inside <p> and <span> tags) from a news story page.
    Returns a dictionary with 'url', 'title', and 'body' as keys.
    """
    result = None
    try:
        response = requests.get(url, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        content = response.text

        soup = BeautifulSoup(content, 'html.parser')
        title = soup.title.string
        body = soup.body

        text = ""

        paragraphs = body.find_all("p", text=True)
        for p in paragraphs:
            text = text + p.string.strip() + " "

        paragraphs = body.find_all("span", text=True)
        for p in paragraphs:
            text = text + p.string.strip() + " "

        text = text.replace("^", "")  # needed to not conflict with the field separator

        result = {}
        result["url"] = url
        result["title"] = title
        result["body"] = text
    except Exception as e:
        print("error in {} - {}".format(url, str(e)))
        traceback.print_exc()

    return result


def get_links(url, filter_list):
    """
    Extract individual story urls to be crawled.
    This is usually called to extract links from a main page or sub-page url.
    """
    links = []

    try:
        url_publisher = re.search("^(http|https)://(.*)\..{2,20}.*$", url).group(2)
        if url_publisher.strip() != "":
            publishers = url_publisher.split(".")
            publishers = [p.lower() for p in publishers]
            last_publisher = publishers[-1]

            pattern_story = re.compile("^(http|https)://.*{}[\.\w\-]*/.+/?$".format(last_publisher))

            response = requests.get(url, timeout=REQUEST_TIMEOUT)
            response.raise_for_status()
            content = response.text

            soup = BeautifulSoup(content, 'html.parser')
            body = soup.body
            for a in body.find_all('a', href=True):
                link = a.get("href").lower()
                if pattern_story.match(link):
                    proceed = False
                    for publisher in publishers:
                        if publisher in link:
                            proceed = True
                    if proceed is True:
                        if len(filter_list) > 0:
                            proceed = False
                            for f in filter_list:
                                if f in link:
                                    proceed = True
                        if proceed is True:
                            links.append(link)

    except Exception as e:
        print("error in {} - {}".format(url, str(e)))
        traceback.print_exc()
    return links


def main():
    # argument parsing
    args = parse_args()
    host = args["host"]
    port = args["port"]
    database = args["database"]
    collection = args["collection"]

    if host is None:
        host = DB_HOST
    if port is None:
        port = DB_PORT
    if database is None:
        database = DB_NAME
    if collection is None:
        collection = DB_COLLECTION

    filters = args["filter"]

    filter_list = []
    if filters is not None:
        filter_list = filters.split(",")
        filter_list = [f.strip() for f in filter_list]

    directory = args["directory"]
    filename = args["filename"]

    # check if directory exists, and if not, exit
    if not os.path.isdir(directory):
        raise Exception("The directory '{}' does not exist".format(directory))

    # generate full file path (directory + filename)
    file_path = os.path.join(directory, filename)

    urls_crawled = urls_crawled_in_dir(directory)

    urls_to_crawl = get_urls_from_mongodb(host, port, database, collection)

    with open(file_path, "a") as file_output:
        wait_and_lock_file(file_output)
        for url in urls_to_crawl:
            if url not in urls_crawled:
                links = get_links(url, filter_list)
                for link in links:
                    if link not in urls_crawled:
                        result = crawl(link)
                        if result is not None:
                            save_result(file_output, result)
                        urls_crawled.add(link)
        unlock_file(file_output)


if __name__ == "__main__":
    main()
