import unittest
import io

from pypeline import pubmed

class MockS3Object(object):
    def __init__(self, key, content=b""):
        self.key = key
        self.content = content

    def upload_fileobj(self, content):
        self.uploaded_content = content.getvalue().decode('utf-8')

    def get(self):
        return {
            'Body': io.BytesIO(self.content)
        }

class MockS3ObjectList(object):
    def __init__(self, objects):
        self.objects = objects

    def filter(self, Prefix):
        self.prefix_used = Prefix
        return self.objects

class MockS3Bucket(object):
    def __init__(self, object_list=[]):
        self.objects = object_list

    def Object(self, key):
        self.object_accessed = MockS3Object(key)
        return self.object_accessed

class PubmedTest(unittest.TestCase):
    def test_mark_delete(self):
        bucket = MockS3Bucket()
        pubmed.ArticleDir(bucket, "12345678").mark_deleted()
        self.assertEqual("12345678/deleted.pmids.xml", bucket.object_accessed.key)
        self.assertIn("<DeletedFromPubmed/>", bucket.object_accessed.uploaded_content)

    def test_fetch(self):
        candidates = MockS3ObjectList([
            MockS3Object("88888888/pubmed19n1111.xml", content=b"no"),
            MockS3Object("88888888/pubmed19n0999.xml", content=b"no"),
            MockS3Object("88888888/pubmed19n3333.xml", content=b"yes"),
            MockS3Object("88888888/pubmed19n2222.xml", content=b"no")
        ])
        bucket = MockS3Bucket(object_list=candidates)
        xml, key = pubmed.ArticleDir(bucket, "88888888").fetch()
        self.assertEqual("yes", xml)
        self.assertEqual("pubmed19n3333.xml", key)
        self.assertEqual("88888888/", candidates.prefix_used)

    def test_file_number(self):
        fn = pubmed.ArticleDir.file_number("pubmed19n1111.xml")
        self.assertEqual(fn, 191111)

    def test_choose_latest_invalid(self):
        candidates = [
            MockS3Object("dummy")
        ]
        with self.assertRaises(IndexError):
            pubmed.ArticleDir.choose_latest(candidates)

    def test_choose_latest_single(self):
        candidates = [
            MockS3Object("12345678/pubmed19n1111.xml")
        ]
        chosen = pubmed.ArticleDir.choose_latest(candidates)
        self.assertEqual(chosen.key, "12345678/pubmed19n1111.xml")

    def test_choose_latest_multi(self):
        candidates = [
            MockS3Object("12345678/pubmed19n1111.xml"),
            MockS3Object("12345678/pubmed19n0999.xml"),
            MockS3Object("12345678/pubmed19n3333.xml"),
            MockS3Object("12345678/pubmed19n2222.xml")
        ]
        chosen = pubmed.ArticleDir.choose_latest(candidates)
        self.assertEqual(chosen.key, "12345678/pubmed19n3333.xml")

    def test_choose_latest_deleted(self):
        candidates = [
            MockS3Object("12345678/deleted.pmids.xml"),
            MockS3Object("12345678/pubmed19n0999.xml"),
            MockS3Object("12345678/pubmed19n3333.xml"),
            MockS3Object("12345678/pubmed19n2222.xml")
        ]
        chosen = pubmed.ArticleDir.choose_latest(candidates)
        self.assertEqual(chosen.key, "12345678/deleted.pmids.xml")
