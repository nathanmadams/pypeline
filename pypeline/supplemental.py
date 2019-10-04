
import datetime
import tempfile
import hashlib
import base64
import json

def pmid_from_key(key):
    return key.split("/")[2]

class ArticleDir(object):
    ORIGINALS_DIR = "originals"
    FILES_DIR = "files"
    METADATA_DIR = "metadata"

    def __init__(self, bucket, pmid):
        assert bucket.name, "bucket param is required. it should be a boto3 bucket instance"
        self.bucket = bucket
        int(pmid)
        self.pmid = pmid

    def all_file_metadata(self):
        return list(self.bucket.objects.filter(Prefix="{}/{}/{}/".format(self.METADATA_DIR, self.ORIGINALS_DIR, self.pmid)))

    def acquire(self, agent, username, link_text, http_response):
        assert agent, "agent param is required. it should describe the scraping program being used."
        self.agent = agent
        assert username, "username param is required. it should describe the person running this program."
        self.username = username
        http_response.raise_for_status()
        md5 = hashlib.md5()
        size = 0
        with tempfile.TemporaryFile() as temp_file:
            for chunk in http_response.iter_content(chunk_size=8192):
                if chunk:
                    temp_file.write(chunk)
                    md5.update(chunk)
                    size = size + len(chunk)
            temp_file.seek(0)
            key = '/'.join([self.FILES_DIR, self.ORIGINALS_DIR, str(self.pmid), md5.hexdigest()])
            metadata_key = '/'.join([self.METADATA_DIR, self.ORIGINALS_DIR, str(self.pmid), md5.hexdigest()])
            object_opts = {
                'Key': key,
                'Body': temp_file,
                'ContentLength': size,
                'ContentMD5': base64.b64encode(md5.digest()).decode('utf-8')
            }
            if 'content-encoding' in http_response.headers:
                # the requests library automatically decodes gzip and deflate encodings
                if not http_response.headers['content-encoding'].lower() in ['gzip', 'deflate']:
                    object_opts['ContentEncoding'] = http_response.headers['content-encoding']
            if 'content-type' in http_response.headers:
                object_opts['ContentType'] = http_response.headers['content-type']

            now = datetime.datetime.utcnow().replace(microsecond=0, tzinfo=datetime.timezone.utc).isoformat()
            metadata = {
                "downloaded-from-request-url": http_response.request.url,
                "downloaded-from-response-url": http_response.url,
                "downloaded-by-agent": self.agent,
                "downloaded-by-user": self.username,
                "downloaded-at": now,
                "downloaded-link-text": link_text,
                "key": key,
                "content-length": size,
                "content-md5": base64.b64encode(md5.digest()).decode('utf-8'),
            }
            for header in ['content-disposition', 'date', 'content-encoding', 'content-type']:
                if header in http_response.headers:
                    metadata["downloaded-header-" + header] = http_response.headers[header]
            self.bucket.put_object(Key=metadata_key, Body=json.dumps(metadata))
            self.bucket.put_object(**object_opts)


