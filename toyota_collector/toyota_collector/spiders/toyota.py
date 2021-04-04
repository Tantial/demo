import scrapy
from scrapy_splash import SplashRequest
from ..items import ToyotaCollectorItem


class ToyotaSpider(scrapy.Spider):
    name = 'toyota'
    allowed_domains = ['larryhmillertoyota.com']
    start_urls = ['https://www.larryhmillertoyota.com/new-inventory/index.htm',
                  'https://www.larryhmillertoyota.com/used-inventory/index.htm'
                  ]

    def start_requests(self):
        for url in self.start_urls:
            yield SplashRequest(url=url, callback=self.parse, endpoint='render.html')

    def parse(self, response):
        data_location = response.xpath('//ul[@class="inventoryList data full list-unstyled"]')
        for car in data_location:
            car_page_url = car.xpath('.//*/a[@class="url"]/@href').extract_first()
            yield SplashRequest(url=response.urljoin(car_page_url), callback=self.process_car_page)

        next_page = response.xpath('//a[@rel="next"]/@href').extract_first()
        # absolute_next_page_url = response.urljoin(next_page)
        yield SplashRequest(url=response.urljoin(next_page), callback=self.parse)

    def process_car_page(self, response):
        vehicle_details = ToyotaCollectorItem()

        data_field_1 = response.xpath('//*[@id="vehicle-title1-app-root"]/div')
        vehicle_details['model'] = ''.join(data_field_1.xpath('.//h1/span[2]/text()').extract())
        vehicle_details['year'] = data_field_1.xpath('.//h1/span[1]/span[2]/text()').extract_first()
        vehicle_details['new_or_used'] = data_field_1.xpath('.//h1/span[1]/span[1]/text()').extract_first()
        vehicle_details['vin'] = data_field_1.xpath('.//ul/li[1]/text()[2]').extract_first()
        vehicle_details['stock_num'] = data_field_1.xpath('.//ul/li[2]/text()[2]').extract_first()

        data_field_2 = response.xpath('//*[@id="quick-specs1-app-root"]/dl')
        if vehicle_details['new_or_used'] == 'New':
            vehicle_details['odometer'] = None
            vehicle_details['fuel_economy'] = data_field_2.xpath('.//dd[1]/span/text()').extract_first()
            vehicle_details['exterior_color'] = data_field_2.xpath('.//dd[2]/span/text()').extract_first()
            vehicle_details['interior_color'] = data_field_2.xpath('.//dd[3]/span/text()').extract_first()
            vehicle_details['body'] = data_field_2.xpath('.//dd[4]/span/text()').extract_first()
            vehicle_details['transmission'] = data_field_2.xpath('.//dd[5]/span/text()').extract_first()
            vehicle_details['drivetrain'] = data_field_2.xpath('.//dd[6]/span/text()').extract_first()
            vehicle_details['engine'] = data_field_2.xpath('.//dd[7]/span/text()').extract_first()
        else:
            vehicle_details['odometer'] = int(data_field_2.xpath('.//dd[1]/span/text()').extract_first().replace(' miles', '').replace(',', ''))
            vehicle_details['fuel_economy'] = data_field_2.xpath('.//dd[2]/span/text()').extract_first()
            vehicle_details['exterior_color'] = data_field_2.xpath('.//dd[3]/span/text()').extract_first()
            vehicle_details['interior_color'] = data_field_2.xpath('.//dd[4]/span/text()').extract_first()
            vehicle_details['body'] = data_field_2.xpath('.//dd[5]/span/text()').extract_first()
            vehicle_details['transmission'] = data_field_2.xpath('.//dd[6]/span/text()').extract_first()
            vehicle_details['drivetrain'] = data_field_2.xpath('.//dd[7]/span/text()').extract_first()
            vehicle_details['engine'] = data_field_2.xpath('.//dd[8]/span/text()').extract_first()

        vehicle_details['dealer_notes'] = ''.join(response.xpath('//*[@id="dealernotes1-app-root"]/div/text()').extract())

        yield vehicle_details
