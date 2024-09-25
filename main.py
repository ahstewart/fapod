import requests
import toml
from io import BytesIO
from PIL import Image
from tqdm import tqdm
import numpy as np


def make_apod_request(api_key, start_date='', end_date='', date=''):
    if date != '':
        response = requests.get(
        f"https://api.nasa.gov/planetary/apod?api_key={api_key}&date={date}").json()
    elif start_date != '' and end_date != '':
        response = requests.get(
            f"https://api.nasa.gov/planetary/apod?api_key={api_key}&start_date={start_date}&end_date={end_date}").json()
    else:
        print("Please provide a date or a start and end date")
        return None
    return response


def get_image(image_url):
    response = requests.get(image_url)
    image = Image.open(BytesIO(response.content))
    return image

def get_image_for_s3_upload(image_url):
    response = requests.get(image_url)
    return BytesIO(response.content)

def get_image_file_size(image_url):
    try:
        response = requests.get(image_url)
        img_file = BytesIO()
        image = Image.open(BytesIO(response.content))
        image.save(img_file, 'png', quality='keep')
        return img_file.tell()
    except Exception as e:
        print(f"Error: {e}")
        return 0


if __name__ == "__main__":
    configs = toml.load("config.toml")
    res = make_apod_request(configs['api_key'], start_date='2010-04-16', end_date='2010-06-30')
    images = res.json()

    #get average file size of images from 1995-06-16 to 2010-06-30
    # file_sizes = []
    # for i in tqdm(images):
    #     if i["media_type"] == "image":
    #         image_url = i["hdurl"]
    #         file_sizes.append(get_image_file_size(image_url))

    # average_file_size = np.average(file_sizes)