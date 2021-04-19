# demo

A collection of some samples of my code, to be used for demonstration, such as on a resume. Some programs may have been edited to remove private information.

**CURRENT PROJECT: Scrape all the images and their metadata from https://www.artbreeder.com/browse using Scrapy + Splash. Currently blocked, because Splash isn't triggering all the JavaScript (even when having Splash wait longer, turning off private browsing, adding a user-agent, enabling full page_view, etc.). Once I figure out how to get the page to fully render in Splash, it should be easy to parse using xpath selectors or json.
So far, I've messed around with the lua script, headers, cookies, checked around to learn more about how http works, tried different middleware packages, all kinds of stuff. So far it's still only loading the page headers. I'm having trouble finding where it separates Splash from a regular web browser. When I render the page in Splash, it usually returns either 200 or 304 status codes, and usually one 401 status code somewhere depending on the header configurations.


deadmanrealty_spider and toyota_collector:
  Scrapy projects I created, using Splash (scrapy_splash). deadmanrealty_spider is unique in that it uses login info (just my email address) to log in and collect all available housing information, which would otherwise be blocked. I started running into lots of 504 errors on both projects, which I haven't yet figured out how to bypass (I believe it is due to my settings in Docker, though I haven't tested it yet). 

database.py:
  Uses pymysql to create a database connection that can be used to create simple queries and provide a high-level view of a database connection. Different connection configurations can be added in db_config.yaml

pokemon_image_scrape.py:
  Uses Beautiful Soup to download images from a basic, static page. The structure of the pages are formatted the same from page-to-page, which makes image collection easy.

fandom_wikia_image_downloader_03.py:
  A much more complicated image downloader, using Beautiful Soup. Entering search terms in the beginning will cause the downloader to search for all domains under the fandom_wikia domain. The structure of each domain under the 'fandom umbrella' varies highly, with some being professional, thorough, and clearly structured sites, while others are empty placeholders for someone else's hobby. The downloader seeks to retrieve as many images of characters as it can from each domain in fandom_wikia. This will end up downloading a lot of useless images, but will collect everything that could potentially be relevant. This is meant to be used on a large scale for image collection, running for weeks at a time, so I've also added safety features in case there are internet issues or downloading is broken up into more manageable chunks, so that work isn't duplicated beyond a single domain.

priority_items.py:
  Was an ETL for a project I was working on. There is private information cut out of it, so I'm not sure how legible it is.
