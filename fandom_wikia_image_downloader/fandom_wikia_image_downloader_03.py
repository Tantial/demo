# -*- coding: utf-8 -*-
# %% Imports

import os
import csv
from typing import List, Set
from urllib.request import urlopen, urlretrieve
from urllib.error import HTTPError, URLError
import re
from bs4 import BeautifulSoup, ResultSet
import datetime
import time
from retry import retry
import json

# These modules used for developing and testing. They can be deleted in the final version
from pprint import pprint  # To more easily read the BeautifulSoup object
import random  # To get random samples

from web_scraping_tools import download_image



# %%
DOWNLOAD_DIR = 'C://Users//Elias//Desktop//fandom_wikia_image_downloader//images'
if not os.path.exists(DOWNLOAD_DIR):
    os.mkdir(DOWNLOAD_DIR)

search_new = False

search_terms = ['friends']

#used_search_terms = ['touhou', 'jojo']


# %% Safety and recovery

"""
Confirm that a connection to domain wikia exists.
If the connection goes down, wait 60 seconds and ping again. Continue until a connection works again
Only change here from the 'retry' decorator is that I wanted it to say something when it needed to retry

"""
    
def retry_connection(func, *args, **kwargs):
    try:
        @retry(exceptions=Exception, tries=-1, delay=60)
        def wrapped(*args, **kwargs):
                response = func(*args, **kwargs)
                return response
    except Exception:
        print("Internet issue. Trying again until the internet issue is resolved")
        @retry(exceptions=Exception, tries=-1, delay=60)
        def wrapped(*args, **kwargs):
                response = func(*args, **kwargs)
                return response
    return wrapped

# %% Retrieve domains

@retry_connection
def retrieve_domain_names(search_terms_list: List[str] = ['anime'], pages_deep: int = 100, search_new: bool = True) -> set():
    """
    Open domain.com and search for wikia in the domain domain using each search term given

    Parameters
    ----------
    search_terms_list : List[str], optional
        A list of strings used to search domain. The default is ['anime'].
    pages_deep : int, optional
        An integer representing how many pages deep to search. The default is 100, but will stop
        if no pages are found


    Returns
    -------
    set()
        All wikias found under the domain domain.

    """
    domain_names: set = set()

    # Read everything in a previous domain page list
    if os.path.exists('found_domains.csv'):
        with open('found_domains.csv', mode='r') as pages:
            reader = csv.reader(pages, delimiter='\n')
            for line in reader:
                if line:
                    domain_names.add(str(line[0]))
            pages.close()

    # Find new pages
    if search_new == True:
        for search_term in search_terms_list:
            search_term = search_term.replace(" ", "_")
            for pagenum in range(1, pages_deep):
                print('Opening page ' + str(pagenum) + ' on domain wikia for search term: ' + search_term)
                # If no domains are found, try changing the url being opened below to "https://community-search.fandom.com/wiki/Special:Search?search="
                anime_search_page: urlopen = urlopen('https://ucp.fandom.com/wiki/Special:SearchCommunity?query=' + search_term + '&page=' + str(pagenum))
                anime_search_soup: BeautifulSoup = BeautifulSoup(anime_search_page, 'html.parser')
                domains: ResultSet = anime_search_soup.find_all('a', {'href': re.compile('https://((?!www|community-search|anime-database).)*\.fandom\.com/$'), 'class': 'result-link'})
                if len(domains) == 0:
                    print("domain page for search '" + search_term + "' ended at page " + str(pagenum))
                    break
                for link in domains:
                    print('Found a link to ' + str(link.getText()))
                    domain_names.add(link['href'])
    # Save domains to file
    with open('found_domains.csv', mode='w') as found_pages:
        writer = csv.writer(found_pages, delimiter="\n")
        writer.writerow(page for page in domain_names)
    print("All found domain links added")

    return domain_names

# Get the domain addresses
domain_list = retrieve_domain_names(search_terms, pages_deep=100, search_new=search_new)
# sample_domain_list = random.sample(domain_list, 10)

def filter_used_domains(domains_list: List[str]):
    """
    Reads the domains in collected_domains and removes each of them from search_terms_list

    Returns
    -------
    None.

    """
    if not os.path.exists('collected_domains.csv'):
        with open('collected_domains.csv', mode='w'):
            pass
    with open('collected_domains.csv', mode='r') as cd:
        reader = csv.reader(cd, delimiter='\n')
        for row in reader:
            if row:
                if row[0] in domains_list:
                    domains_list.remove(row[0])
        return domains_list

# Filter to the domains that have already been searched (collected_domains.csv)
domain_list = filter_used_domains(domain_list)


# %% Collect and download from page

# To download from a character or gallery page, call the collect_from_images_page function

def check_filetype(absolute_url: str):
    formats = ['png', 'jpg', 'jpeg', 'bmp']
    for fmt in formats:
        if re.match('.*\.' + fmt, absolute_url, re.IGNORECASE):
            return fmt

@retry_connection
def collect_from_images_page(image_page: str, domain_name:str):
    """
    Download all images on a domain wiki character or gallery page
    
    Images page example: https://pokemon.domain.com/wiki/Professor_Oak_(anime)
                         https://fairytail.domain.com/wiki/Happy/Anime_Gallery
                         https://berserk.domain.com/wiki/Gallery:Slan !!! Needs attention
    domain_name example: https://(this_is_the_domain_name).domain.com/

    Returns
    -------
    None.

    """
    # Will fail if the url does not exist. Not sure how it gets bad urls yet but it has happened
    # AttributeError is for if there isn't an image in the normal spot
    try:
        html: urlopen = urlopen(image_page)
        soup = BeautifulSoup(html, 'html.parser')
        page_name = soup.find('h1', {'class': 'page-header__title'}).get_text()
    except (HTTPError, AttributeError):
        return
    # Wiki article block (so not all the borders and website headers and stuff. Just the article)
    l1 = soup.find('div', {'id':'WikiaArticle'})
    if l1 is None:
        l1 = soup.find('div', {'class':'WikiaArticle'})
    # All the tags where there is an image source with a png, a jpg, a jpeg, or a bmp image
    l2 = l1.find_all('img', {'src':re.compile('.*\.(png|jpe?g|bmp).*', re.IGNORECASE)})
    # The absolute url where the image is
    l3 = [tag['src'] for tag in l2]
    for scale in l3:
        l3[l3.index(scale)] = re.sub('latest/.*\?cb', 'latest/?cb', scale)
    
    for abs_url in l3:
        fmt = check_filetype(abs_url)
        # May fail if there are no images in the page
        try:
            download_image(img_src=abs_url, img_format=fmt, domain_name=domain_name, download_dir=DOWNLOAD_DIR)
        except (HTTPError, ValueError):
            continue


# %%

@retry_connection
def get_domain_categories(domain_base_link: str):
    """
    Take in the url in the domain_base_link string and return a list of all urls for characters in that domain domain

    Parameters
    ----------
    domain_link : str
        The full url of the main page for a domain. Ex. 'https://llama.domain.com/'

    Returns
    -------
    List[str]
        A list of all Characters pages under the domain domain. Ex:
            ['https://llama.domain.com//wiki/Category:Character'
             'https://llama.domain.com//wiki/Category:Characters'
             'https://llama.domain.com//wiki/Category:Minor_Characters'
             'https://llama.domain.com//wiki/Category:Unknown_Characters'
             'https://llama.domain.com//wiki/Category:Unseen_Characters']

    """
    print("Collecting character and gallery pages for " + domain_base_link)
    character_categories_list = []

    topics = ['character', 'gallery']
    for t in topics:
        characters_category_search = urlopen(domain_base_link + 'index.php?title=Special%3ACategories&from=' + t)
        characters_category_search_soup = BeautifulSoup(characters_category_search, 'html.parser')
        category_pages = characters_category_search_soup.find('ul', {'class': ''}).find_all('a', {'href': re.compile('/wiki/Category:.*(Character|Gallery).*')})
        for link in category_pages:
            # link['href'][1:] is just the wiki/Chategory:Characters stuff, just without the '/' in front
            character_categories_list.append(link['href'][1:])
    for page in character_categories_list:
        character_categories_list[character_categories_list.index(page)] = domain_base_link + page
    ccl_clean = []
    # I think this removes duplicates?
    [ccl_clean.append(x) for x in character_categories_list if x not in ccl_clean]

    return ccl_clean


def get_domain_name(domain_base_link: str):
    """
    Parse the domain base link into the name of the domain.
    
    Ex. IN: 'https://godofhighschool.fandom.com/'
        OUT: godofhighschool

    """
    domain_name = re.sub('/wiki/.*', '', domain_base_link)
    domain_name = domain_name.replace('https://', '')
    domain_name = domain_name.replace('.fandom.com/', '')
    return domain_name

@retry_connection
def get_character_pages(domain_categories: list, domain_base_link: str):
    """
    Accepts a list of Category pages and returns a list of character pages it found

    Parameters
    ----------
    domain_categories : list
        DESCRIPTION.
    domain_base_link : str
        DESCRIPTION.

    Returns
    -------
    full_character_pages : TYPE
        DESCRIPTION.

    """
    full_character_pages = set()
    for category in domain_categories:
        try:
            html: urlopen = urlopen(category)
            soup: BeautifulSoup = BeautifulSoup(html, 'html.parser')
            characters_block = soup.find('div', {'class': 'category-page__members'})
            character_page_links = characters_block.find_all('a', {'href': re.compile('/wiki/.*'), 'class': re.compile('category-page__member-.*')})
            character_pages = [x['href'] for x in character_page_links]
            pages_to_add = [domain_base_link + char_link for char_link in character_pages]
            for page in pages_to_add:
                full_character_pages.add(page)
        except (AttributeError, HTTPError):
            full_character_pages = [None]
    full_character_pages = list(full_character_pages)
    return full_character_pages


def sort_category_from_character(character_page_urls, pattern):
    characters = []
    categories = []
    for url in character_page_urls:
        if url:
            if re.match(pattern, url):
                categories.append(url)
            elif re.match(re.compile('.*File:.*'), url):
                continue
            else:
                characters.append(url)
    return characters, categories


@retry_connection
def gather_remaining_pages(character_page_urls: List[str], domain_base_link: str):
    """
    Takes in the initial list of links from a category search and finds the remaining category and character links that it can find
    """

    # Character pages that have been collected
    collected_pages: Set[str] = set()
    # Categories that need to be opened and their links viewed
    categories_to_check: List[str] = []
    # Categories that have been checked and should not be checked again
    checked_categories: List[str] = []

    # Establish the pattern that Category urls follow
    category_pattern = re.compile('.*Category:.*')

    # Sort all the character pages from the category pages. This needs to be run once the first time
    sorted_pages = list(sort_category_from_character(character_page_urls, category_pattern))
    for x in sorted_pages[0]:
        collected_pages.add(x)
    for y in sorted_pages[1]:
        categories_to_check.append(y)

    # Iterate through the collected categories in search of new character pages
    while len(categories_to_check) > 0:
        # I think this should operate the same as a for loop
        category_url = categories_to_check.pop(0)
        # Do some checks
        # If the category has already been checked, move to the next category
        if category_url in checked_categories:
            continue
        # If it's a category page that has not been checked...
        elif re.match(category_pattern, category_url) and category_url not in checked_categories:
            # ...Gets that category's links and sorts them
            pages_to_sort = get_character_pages([category_url], domain_base_link)
            characters, categories = sort_category_from_character(pages_to_sort, category_pattern)
            for char in characters:
                collected_pages.add(char)
            for cat in categories:
                if cat not in checked_categories:
                    categories_to_check.append(cat)
        checked_categories.append(category_url)
    
    return list(collected_pages)


def get_one_domain(domain_base_link: str):
    domain_name = get_domain_name(domain_base_link)
    domain_categories = get_domain_categories(domain_base_link)
    character_page_urls = get_character_pages(domain_categories, domain_base_link)
    all_character_pages = gather_remaining_pages(character_page_urls, domain_base_link)

    for image_page in all_character_pages:
        collect_from_images_page(image_page, domain_name)

    with open('collected_domains.csv', mode='a', newline='') as collected_domains:
        writer = csv.writer(collected_domains, delimiter=',')
        writer.writerow([domain_base_link])


# %%


for d in domain_list:
    get_one_domain(d)
