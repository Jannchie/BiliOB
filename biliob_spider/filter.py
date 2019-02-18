from scrapy.dupefilter import RFPDupeFilter


class CloseDupefilter(RFPDupeFilter):
    def request_seen(self, request):
        return False
