import unittest
import io
import time
import json

import requests

from pypeline import supplemental

class MockS3UploadBucket(object):
    def __init__(self, name="dummy"):
        self.name = name
        self.uploaded_kwargs = []

    def put_object(self, **kwargs):
        self.uploaded_kwargs.append(kwargs)

class SupplementalTest(unittest.TestCase):
    def test_acquire(self):
        bucket = MockS3UploadBucket()
        a = supplemental.ArticleDir(bucket, "12345678")

        resp = requests.get("https://genomenon-open-data.s3.amazonaws.com/sPc73m9/BasicsPresentation.pdf")
        a.acquire("robot_overlord", "mctesterson", "fascinating data", resp)

        f = bucket.uploaded_kwargs[0]
        self.assertEqual("12345678/", f['Key'][:9])
        self.assertEqual("application/pdf", f['ContentType'])
        self.assertTrue(f['ContentLength'])
        md = json.loads(bucket.uploaded_kwargs[1]['Body'])
        self.assertEqual("application/pdf", md['downloaded-header-content-type'])
        self.assertEqual("robot_overlord", md['downloaded-by-agent'])
        self.assertEqual("mctesterson", md['downloaded-by-user'])
        self.assertEqual("https://genomenon-open-data.s3.amazonaws.com/sPc73m9/BasicsPresentation.pdf", md['downloaded-from-response-url'])
        self.assertEqual("fascinating data", md['downloaded-link-text'])
        self.assertEqual(f['Key'], md['key'])