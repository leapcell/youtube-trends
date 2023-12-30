import requests
from leapcell import Leapcell
import time
import logging
import datetime
import sys
import copy
import os
from flask import Flask, render_template, request, redirect
import random
import threading

logging.basicConfig(
    stream=sys.stdout,
    level=logging.INFO,
    format="%(asctime)s %(name)s %(levelname)s %(message)s",
)

app = Flask(__name__)

key = os.environ.get("YOUTUBE_KEY", "")
leapclient = Leapcell(
    os.environ.get("LEAPCELL_API_KEY"),
)

table = leapclient.table("issac/youtube-trends", "tbl1739282705796497408")
headers = {"Accept": "application/json"}


category = {
    "1": "Film & Animation",
    "2": "Autos & Vehicles",
    "10": "Music",
    "15": "Pets & Animals",
    "17": "Sports",
    "18": "Short Movies",
    "19": "Travel & Events",
    "20": "Gaming",
    "21": "Videoblogging",
    "22": "People & Blogs",
    "23": "Comedy",
    "24": "Entertainment",
    "25": "News & Politics",
    "26": "Howto & Style",
    "27": "Education",
    "28": "Science & Technology",
    "29": "Nonprofits & Activism",
    "30": "Movies",
    "31": "Anime/Animation",
    "32": "Action/Adventure",
    "33": "Classics",
    "34": "Comedy",
    "35": "Documentary",
    "36": "Drama",
    "37": "Family",
    "38": "Foreign",
    "39": "Horror",
    "40": "Sci-Fi/Fantasy",
    "41": "Thriller",
    "42": "Shorts",
    "43": "Shows",
    "44": "Trailers",
}

target_regions = [
    "Germany",
    "Australia",
    "Canada",
    "United Kingdom",
    "Ireland",
    "Singapore",
    "United States",
    "Spain",
    "Mexico",
    "France",
    "Italy",
    "Japan",
    "South Korea",
    "Netherlands",
    "Poland",
    "Brazil",
    "",
]


def get_trends_video(region: str, category: str):
    url = "https://www.googleapis.com/youtube/v3/videos"
    response = requests.get(
        url,
        params={
            "part": "contentDetails",
            "chart": "mostPopular",
            "regionCode": region,
            "key": key,
            "videoCategoryId": category,
        },
    )
    return response.json()


def get_video_info(id: str):
    url = "https://www.googleapis.com/youtube/v3/videos"
    response = requests.get(url, params={"part": "snippet", "id": id, "key": key})
    return response.json()


def get_region():
    url = "https://www.googleapis.com/youtube/v3/i18nRegions"
    response = requests.get(url, params={"part": "snippet", "key": key})
    return response.json()


def process_trends_video(region: str, category_id: str, region_name: str):
    now_dt = datetime.datetime.now()  # TODAY
    now = datetime.datetime(now_dt.year, now_dt.month, now_dt.day)
    now_ts = time.mktime(now.timetuple())
    count = (
        table.select()
        .where(
            (table["region"] == region)
            & (table["category"] == category[category_id])
            & (table["retrieve_time"] > now_ts)
        )
        .count()
    )
    if count >= 3:
        return {"items": []}

    trends = get_trends_video(region, category_id)
    if "items" not in trends:
        return {"items": []}
    if len(trends["items"]) == count:
        return {"items": []}

    images = []

    for item in trends["items"]:
        video_id = item["id"]
        video_info = get_video_info(video_id)
        time.sleep(1)
        if len(video_info["items"]) == 0:
            continue
        video_info = video_info["items"][0]
        response = requests.get(video_info["snippet"]["thumbnails"]["high"]["url"])
        if response.status_code != 200:
            logging.error("Failed to download image for video %s", video_id)
        images.append(copy.deepcopy(response.content))

        image = table.upload_file(response.content)

        publishAt = datetime.datetime.strptime(
            video_info["snippet"]["publishedAt"], "%Y-%m-%dT%H:%M:%S%z"
        )
        tags = []
        if "tags" in video_info["snippet"]:
            tags = video_info["snippet"]["tags"]
        record = {
            "title": video_info["snippet"]["title"],
            "description": video_info["snippet"]["description"],
            "cover": image.link(),
            "video_id": video_id,
            "publishAt": time.mktime(publishAt.timetuple()),
            "channel": video_info["snippet"]["channelTitle"],
            "tag": tags,
            "region": region_name,
            "category": category[category_id],
            "url": "https://www.youtube.com/watch?v=" + video_id,
            "retrieve_time": now_ts,
            "channelId": video_info["snippet"]["channelId"],
        }
        table.upsert(
            record,
            on_conflict=["video_id", "retrieve_time", "region"],
        )
    return trends


def retrieve():
    regions = get_region()
    for region in regions["items"]:
        if region["snippet"]["name"] not in target_regions:
            continue
        for key in category.keys():
            process_trends_video(
                region["snippet"]["gl"], key, region_name=region["snippet"]["name"]
            )


@app.route("/process_trends_video")
def process_trends_video_api():
    region = request.args.get("region")
    category_id = request.args.get("category_id")
    region_name = request.args.get("region_name")
    return process_trends_video(region, category_id, region_name)


def call_retrieve_data(region: str, category_id: str, region_name: str):
    r = requests.get(
        "https://issac-youtube-trends-ctdkmhdx.leapcell.dev/process_trends_video",
        params={
            "region": region,
            "category_id": category_id,
            "region_name": region_name,
        },
    )


@app.route("/retrieve")
def retrieve_api():
    regions = get_region()
    logging.info("Retrieve youtube trends video")
    for region in regions["items"]:
        if region["snippet"]["name"] not in target_regions:
            continue
        for key in category.keys():
            threading.Thread(
                target=call_retrieve_data,
                args=(region["snippet"]["gl"], key, region["snippet"]["name"]),
            ).start()
            time.sleep(random.randint(1, 5) * 0.01)

    return {"status": "ok"}


@app.route("/")
def index():
    return """
    <h1>This is not a service, it is a script</h1>
"""


if __name__ == "__main__":
    # retrieve()
    app.run(port=5000, debug=True)
