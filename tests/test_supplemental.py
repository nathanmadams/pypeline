import unittest
import io
import time

import requests

from pypeline import supplemental

class MockS3UploadBucket(object):
    def __init__(self, name="dummy"):
        self.name = name

    def put_object(self, Key, Body, Metadata):
        self.uploaded_key = Key
        self.uploaded_body = Body
        self.uploaded_metadata = Metadata

class SupplementalTest(unittest.TestCase):
    def test_acquire(self):
        bucket = MockS3UploadBucket()
        a = supplemental.ArticleDir(bucket, "12345678", "robot_overlord", "mctesterson")

        resp = requests.get("https://www.ncbi.nlm.nih.gov/pmc/articles/PMC5366041/pdf/nihms843860.pdf")
        a.acquire("fascinating data", resp)

        self.assertEqual("12345678/", bucket.uploaded_key[:9])
        md = bucket.uploaded_metadata
        self.assertEqual("application/pdf", md['downloaded-header-content-type'])
        self.assertEqual("robot_overlord", md['downloaded-by-agent'])
        self.assertEqual("mctesterson", md['downloaded-by-user'])
        self.assertIn("www.ncbi.nlm.nih.gov", md['downloaded-from-url'])
        self.assertEqual("fascinating data", md['downloaded-link-text'])