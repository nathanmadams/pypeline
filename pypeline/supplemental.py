
import datetime
import tempfile
import hashlib
import base64

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
        headers = http_response.headers
        for header, value in headers.items():
            metadata["downloaded-header-" + header.lower()] = value
        md5 = hashlib.md5()
        size = 0
        with tempfile.TemporaryFile() as temp_file:
            for chunk in http_response.iter_content(chunk_size=8192):
                if chunk:
                    temp_file.write(chunk)
                    md5.update(chunk)
                    size = size + len(chunk)
            temp_file.seek(0)
            key = '/'.join([str(self.pmid), md5.hexdigest()])
            object_opts = {
                'Key': key,
                'Body': temp_file,
                'Metadata': metadata,
                'ContentLength': size,
                'ContentMD5': base64.b64encode(md5.digest())
            }
            if 'content-encoding' in headers:
                object_opts['ContentEncoding'] = headers['content-encoding']
            if 'content-type' in headers:
                object_opts['ContentType'] = headers['content-type']
            self.bucket.put_object(**object_opts)


