import scrapy
from scrapy_splash import SplashRequest, SplashFormRequest
from ..items import DeadmanrealtySpiderItem


class DeadmanrealtySpider(scrapy.Spider):
    name = 'deadmanrealty'
    allowed_domains = ['deadmanrealtyofutah.com']
    start_urls = ['https://deadmanrealtyofutah.com/fine/real/estate/saved']  # login page

    def start_requests(self):
        for url in self.start_urls:
            yield SplashRequest(url, callback=self.login)

    def login(self, response):
        yield SplashFormRequest.from_response(response=response, formxpath='//div[3]//tr[1]/td/input', formdata={'name': 'eliasdeadman@gmail.com'}, callback=self.go_to_listings)

    def go_to_listings(self, response):
        listings_url = response.xpath('//*[@id="menu2"]/ul/li[4]/a/@href').extract_first()
        yield SplashRequest(url=response.urljoin(listings_url), callback=self.parse, args={'wait': 0.5, 'timeout': 60})

    def parse(self, response):
        page_locations = response.xpath('//div[@id="ia_contents"]/div[@class="viewgrid"]')
        for box in page_locations:
            page_url = box.xpath('.//a/@href').extract_first()
            # 3 second wait below is to help make sure the address loads. It doesn't always work,
            # but I've noticed that a higher wait returns more results.
            # resource_timeout in 'args' below is to help prevent 504 Gateway errors.
            yield SplashRequest(url=response.urljoin(page_url), callback=self.parse_page, args={'wait': 3})

        next_page = response.xpath('//*[@id="ia_btn_next"]/@href').extract_first()
        # resource_timeout in 'args' below is to help prevent 504 Gateway errors.
        yield SplashRequest(url=response.urljoin(next_page), callback=self.parse, args={'wait': 0.5})

    def parse_page(self, response):
        if 'marketeval' in response.url:
            self.logger.info('Found an eval page at ' + response.url + '. Skipping...')
        else:
            property_details = DeadmanrealtySpiderItem()

            property_details['address'] = response.xpath('//*[@id="ia_address"]/h1/text()').extract_first().replace('\n', '').replace('\t', '')
            raw_detail_keys = [category.replace(':', '') for category in response.xpath('//*[@id="PropDetailItem"]/div[1]/text()').extract()]
            raw_detail_values = response.xpath('//*[@id="PropDetailItem"]/div[2]/text()').extract()
            property_details['details'] = dict(zip(raw_detail_keys, raw_detail_values))

            yield property_details
