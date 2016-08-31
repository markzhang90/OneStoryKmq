import urllib.parse, urllib.request
import json


class HttpProxy:

    def __new__(cls, *args, **kwargs):
        if not hasattr(cls, '_instance'):
            cls._instance = super(HttpProxy, cls).__new__(cls)
        return cls._instance

    def __init__(self):
        pass

    def send_request(self, target_url, data):

        req = None
        try:
            request_para = urllib.parse.urlencode(data).encode('UTF-8')
            url = urllib.request.Request(target_url, request_para)
            req = urllib.request.urlopen(url)
        except Exception as e:
            print(e)

        return req
