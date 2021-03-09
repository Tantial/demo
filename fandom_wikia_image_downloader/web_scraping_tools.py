# -*- coding: utf-8 -*-
import os
import datetime
from urllib.request import urlopen, urlretrieve
import re


#download_dir = ''

def check_filetype(absolute_url: str):
    formats = ['png', 'jpg', 'jpeg', 'bmp']
    for fmt in formats:
        if re.match('.*\.' + fmt, absolute_url, re.IGNORECASE):
            return fmt

def download_image(img_src: str, img_format: str, domain_name: str, download_dir:str, img_name:str=None):
    """
    Download the image from the absolute image source

    Parameters
    ----------
    Absolute url of the image source. Ex.'https://vignette.wikia.nocookie.net/pokemon/images/b/bf/Sammy_Oak_concept_art.JPG/revision/latest/scale-to-width-down/185?cb=20200619103726'
    download_location : str
        Absolute path to download to. Ex. 
    domain_name : str
        Name of the domain under domain wikia. Ex. 'pokemon' (such as pokemon.domain.com...)
    page_name : str
        Name of the character or gallery page.

    Returns
    -------
    None.

    """
    # If there is an image source
    if img_src is not None:
        # Name the image the current timestamp
        if img_name:
            img_name = img_name + '.' + img_format
        else:
            img_name = str(datetime.datetime.now().strftime("%Y%m%d-%H%M%S")) + '.' + img_format
        # Ensure there is a filepath to download to
        #page_name = page_name.replace('/', '-')
        #page_name = page_name.replace(':', ' ')
        if not os.path.exists(download_dir + '\\' + domain_name):
            os.mkdir(download_dir + '\\' + domain_name)
        #if not os.path.exists(DOWNLOAD_DIR + '\\' + domain_name + '\\' + page_name):
        #    os.mkdir(DOWNLOAD_DIR + '\\' + domain_name + '\\' + page_name)
        # Rename the download path
        #download_path = DOWNLOAD_DIR + '\\' + domain_name + '\\' + page_name + '\\' + img_name
        download_path = download_dir + '\\' + domain_name + '\\' + img_name
        if os.path.exists(download_path):
            count = 0
            img_name = str(datetime.datetime.now().strftime("%Y%m%d-%H%M%S")) + '_(0).' + img_format
            while os.path.exists(download_path):
                img_name = img_name.replace('_(' + str(count) + ').', '_(' + str(count + 1) + ').')
                img_name = img_name.replace('_(0).', '.')
                download_path = download_dir + '\\' + domain_name + '\\' + img_name
                count += 1
        
        # Download the image to the download path specified
        urlretrieve(img_src, download_path)
        #time.sleep(1)

