from scrapy.dupefilters import RFPDupeFilter


class CloseDupefilter(RFPDupeFilter):
    def request_seen(self, request):
        return False
