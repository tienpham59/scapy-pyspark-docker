import scrapy
from chothuenha_crawler.items import ChothuenhaCrawlerItem


class ChothuenhaSpiderSpider(scrapy.Spider):
    name = "chothuenha_spider"
    allowed_domains = ["chothuenha.com.vn"]
    start_urls = ["https://chothuenha.com.vn/cho-thue-nha/?page1"]

    def __init__(self, start_page=1, end_page=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.start_page = int(start_page)
        self.end_page = int(end_page) if end_page is not None else None
        self.current_page = self.start_page

    def start_requests(self):
        url = f'https://chothuenha.com.vn/cho-thue-nha/?page={self.start_page}'
        yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):
        # Lấy danh sách các liên kết chi tiết
        detail_links = response.xpath("//div[@class='dv-bds']/div/h3/a/@href").getall()
        for link in detail_links:
            yield response.follow(link, self.parse_detail)

        # Lấy link tiếp theo để cào tiếp nếu có và nếu chưa đạt trang kết thúc
        if self.end_page is None or self.current_page < self.end_page:
            self.current_page += 1
            next_page = f'https://chothuenha.com.vn/cho-thue-nha/?page={self.current_page}'
            yield response.follow(next_page, self.parse)

    def parse_detail(self, response):
        # Xử lý dữ liệu từ trang chi tiết 
        item = {
                'Posting_id': response.xpath('//div[@class="dv-main-left"]/div[7]/div[2]/div[4]/b/text()').get(),
                'Title': response.xpath('//h1[@class="h2-title inhoa"]/text()').get(),
                'Address': response.xpath('//p[@class="p-map"]/text()').get(),
                'City': response.xpath('//div[@class="dv-bre"]/a[2]/text()').get(),
                'District': response.xpath('//div[@class="dv-bre"]/a[3]/text()').get(),
                'Ward': response.xpath('//div[@class="dv-bre"]/a[4]/text()').get(),
                'Street': response.xpath('//div[@class="dv-bre"]/a[5]/text()').get(),
                'Rental_price': response.xpath('//div[@class="dv-bds-ct"]/span[@class="price"]/text()').get(),
                'Room_area': response.xpath('//div[@class="dv-bds-ct"]/span[2]/text()').get(),
                'Bedrooms': response.xpath('//div[@class="dv-bds-ct"]/span[3]/text()').get(),
                'Toilets': response.xpath('//div[@class="dv-bds-ct"]/span[4]/text()').get(),
                'poster': response.xpath('//div[@class="dv-info-mb"]//span[@class="spname"]/b/text()').get(),
                'Posting_date': response.xpath('//div[@class="dv-main-left"]/div[7]/div[2]/div/b/text()').get(),
                'Type_of_listing': response.xpath('//div[@class="dv-main-left"]/div[7]/div[2]/div[3]/b/text()').get(),
                'View': response.xpath('//div[@class="dv-main-left"]/div[7]/div[2]/div[5]/b/text()').get(),
                'Describe': response.xpath('//div[@class="dv-main-left"]/div[3]/div[2]/text()').getall(),
            }
            
            # Bạn có thể kiểm tra và xử lý dữ liệu ở đây nếu cần
        yield item