
import datetime
import tempfile
import hashlib

class ArticleDir(object):

    def __init__(self, bucket, pmid, agent, username):
        assert bucket.name, "bucket param is required. it should be a boto3 bucket instance"
        self.bucket = bucket
        int(pmid)
        self.pmid = pmid
        assert agent, "agent param is required. it should describe the scraping program being used."
        self.agent = agent
        assert username, "username param is required. it should describe the person running this program."
        self.username = username

    def acquire(self, link_text, http_response):
        http_response.raise_for_status()
        now = datetime.datetime.utcnow().replace(microsecond=0, tzinfo=datetime.timezone.utc).isoformat()
        metadata = {
            "downloaded-from-url": http_response.url,
            "downloaded-by-agent": self.agent,
            "downloaded-by-user": self.username,
            "downloaded-at": now,
            "downloaded-link-text": link_text,
        }
        for header, value in http_response.headers.items():
            metadata["downloaded-header-" + header.lower()] = value
        md5 = hashlib.md5()
        with tempfile.TemporaryFile() as temp_file:
            for chunk in http_response.iter_content(chunk_size=8192):
                if chunk:
                    temp_file.write(chunk)
                    md5.update(chunk)
            temp_file.seek(0)
            key = '/'.join([str(self.pmid), md5.hexdigest()])
            self.bucket.put_object(Key=key, Body=temp_file, Metadata=metadata)


