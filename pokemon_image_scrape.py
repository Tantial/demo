# -*- coding: utf-8 -*-
"""
Created on Mon Jun 24 19:43:47 2019

@author: Elias

This script searches serebii.net's Pokemon Black/White Pokedex to download each
pokemon's sprite image. What you do with that, it's up to you. Maybe use them
for a GAN?
"""
# %% Imports
import os
import time
from urllib.request import urlopen, urlretrieve
import re
from bs4 import BeautifulSoup


# %%
class PokemonImageScrape():
    """
    This class uses a simple regular expression to locate the pokemon's image
    in the html and download it to the current folder.
    """

    # Observes the current directory. This is to check if the folder and files
    # exist already, and creates them if they do not exist.
    curdir = os.path.dirname(__file__)
    pokemon_sprites_dir = os.path.join(curdir, 'pokemon_sprites')
    if not os.path.exists(pokemon_sprites_dir):
        os.mkdir(pokemon_sprites_dir)

    def __init__(self, page_url: str) -> None:
        """
        :param page_url: The entire URL string after 'serebii.net'
        """
        self.html: urlopen = urlopen(f'https://serebii.net{page_url}')
        self.soup: BeautifulSoup = BeautifulSoup(self.html, 'html.parser')
        self.pages: set = set()
        self.get_links(self.soup)
        self.visit_page()

    def get_links(self, page_url: urlopen) -> None:
        """
        Accepts the BeautifulSoup object passed into it, searches for all
        instances other Pokedex pages (url substrings starting with pokedex-bw)
        and adds each of the different substrings into a set that will be
        iterated through in the `visit_page` method.
        """
        poke_list: BeautifulSoup = page_url.find_all(
            'option', {'value': re.compile('/pokedex-bw/[0-9]*.shtml')})
        for link in poke_list:
            self.pages.add(link['value'])

    def collect_image(self, cur_page: BeautifulSoup) -> None:
        """
        Locates the location of the page's pokemon image and downloads it to
        the directory that this script is working in. The file's name will be
        the pokemon's National Dex ID with a .png filetype.

        To instead download the Shiny pokemon images, simply replace
        '/blackwhite/pokemon/' with '/Shiny/BW/' in the img declaration.
        """
        img: BeautifulSoup = cur_page.find(
            'img', {'src': re.compile('/blackwhite/pokemon/[0-9]*\\.png')})
        img_name: str = img.attrs['src'][20:23]+'.png'
        # Creates the .png file if it does not exist already.
        if not os.path.exists(os.path.join(self.pokemon_sprites_dir,
                                           img_name)):
            urlretrieve('https://serebii.net'+img['src'],
                        self.pokemon_sprites_dir + '//' + img_name)
            # Self-imposed timer to throttle the scraper's web activity and
            # avoid overloading the servers. Probably not necessary here, but
            # is nice to do.
            time.sleep(5)

    def visit_page(self) -> None:
        """
        Iterates through each page in the Pages set (collected in the :meth:
        `get_links` method). It will print whichever pokedex page it is
        currently on and download the pokemon's regular image with the :meth:
        `collect_image` method.
        """
        for page in self.pages:
            print(page)
            self.html: urlopen = urlopen(f'https://serebii.net{page}')
            self.soup: BeautifulSoup = BeautifulSoup(self.html, 'html.parser')
            self.collect_image(self.soup)


# Runs the scraper. You can change the starting point to any valid pokemon ID,
# it will get the same result.
PokemonImageScrape('/pokedex-bw/001.shtml')
