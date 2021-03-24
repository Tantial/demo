# demo
A collection of some samples of my code, to be used for demonstration, such as on a resume. Some programs may have been edited to remove private information.

database.py:
  Uses pymysql to create a database connection that can be used to create simple queries and provide a high-level view of a database connection. Different connection configurations can be added in db_config.yaml

pokemon_image_scrape.py:
  Uses Beautiful Soup to download images from a basic, static page. The structure of the pages are formatted the same from page-to-page, which makes image collection easy.

fandom_wikia_image_downloader_03.py:
  A much more complicated image downloader, using Beautiful Soup. Entering search terms in the beginning will cause the downloader to search for all domains under the fandom_wikia domain. The structure of each domain under the 'fandom umbrella' varies highly, with some being professional, thorough, and clearly structured sites, while others are empty placeholders for someone else's hobby. The downloader seeks to retrieve as many images of characters as it can from each domain in fandom_wikia. This will end up downloading a lot of useless images, but will collect everything that could potentially be relevant. This is meant to be used on a large scale for image collection, running for weeks at a time, so I've also added safety features in case there are internet issues or downloading is broken up into more manageable chunks, so that work isn't duplicated beyond a single domain.

priority_items.py:
  Was an ETL for a project I was working on. There is private information cut out of it, so I'm not sure how legible it is.

Current Project: Find and crawl a website using Scrapy, utilizing all parts of the framework to handle cookies, login credentials, data storage, dynamic population from Javascript, etc.
