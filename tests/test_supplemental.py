import unittest
import io
import time

import requests

from pypeline import supplemental

class MockS3UploadBucket(object):
    def __init__(self, name="dummy"):
        self.name = name

    def put_object(self, **kwargs):
        self.uploaded_kwargs = kwargs

class SupplementalTest(unittest.TestCase):
    def test_acquire(self):
        bucket = MockS3UploadBucket()
        a = supplemental.ArticleDir(bucket, "12345678")

        resp = requests.get("https://genomenon-open-data.s3.amazonaws.com/sPc73m9/BasicsPresentation.pdf")
        a.acquire("robot_overlord", "mctesterson", "fascinating data", resp)

        self.assertEqual("12345678/", bucket.uploaded_kwargs['Key'][:9])
        self.assertEqual("application/pdf", bucket.uploaded_kwargs['ContentType'])
        self.assertTrue(bucket.uploaded_kwargs['ContentLength'])
        md = bucket.uploaded_kwargs['Metadata']
        self.assertEqual("application/pdf", md['downloaded-header-content-type'])
        self.assertEqual("robot_overlord", md['downloaded-by-agent'])
        self.assertEqual("mctesterson", md['downloaded-by-user'])
        self.assertEqual("https://genomenon-open-data.s3.amazonaws.com/sPc73m9/BasicsPresentation.pdf", md['downloaded-from-url'])
        self.assertEqual("fascinating data", md['downloaded-link-text'])