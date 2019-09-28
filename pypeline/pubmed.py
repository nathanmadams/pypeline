import io
import logging

class ArticleDir(object):
    ENCODING = "utf-8"
    XML_HEADER = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
    DTD_HEADER = "<!DOCTYPE PubmedArticle PUBLIC \"-//NLM//DTD PubMedArticle, 1st January 2019//EN\" \"https://dtd.nlm.nih.gov/ncbi/pubmed/out/pubmed_190101.dtd\">"
    DELETE_MARKER_NAME = "deleted.pmids.xml"
    PATH_SEP = "/"

    def __init__(self, bucket, pmid):
        self.bucket = bucket
        self.pmid = pmid

    def mark_deleted(self):
        content = f"{self.XML_HEADER}\n<DeletedFromPubmed/>"
        self.__write_object(self.DELETE_MARKER_NAME, content)

    def save(self, two_digit_year, sequence_number, xml_content):
        content = '\n'.join([self.XML_HEADER, self.DTD_HEADER, xml_content])
        file_name = f"pubmed{two_digit_year:02}n{sequence_number:04}.xml"
        self.__write_object(file_name, content)

    def fetch(self):
        candidates = self.list_versions()
        logging.info(f"found {len(candidates)} XML files (versions) for {self.pmid}")
        chosen = self.choose_latest(candidates)
        if chosen is None:
            logging.info(f"no XML found in S3 for PMID {self.pmid}")
            return None
        logging.info(f"latest XML version for PMID {self.pmid} is {chosen.key}")
        return chosen.get()['Body'].read().decode(self.ENCODING)

    def list_versions(self):
        return list(self.bucket.objects.filter(Prefix="{}/".format(self.pmid)))

    def __write_object(self, file_name, content):
        buffer = io.BytesIO(content.encode(self.ENCODING))
        key = str(self.pmid) + self.PATH_SEP + file_name
        self.bucket.Object(key).upload_fileobj(buffer)
        logging.info(f"finished uploading {key}")

    @classmethod
    def parse_file_number(cls, file_name):
        return int(file_name[6:8]) * 10000 + int(file_name[9:13])

    @classmethod
    def choose_latest(cls, candidates):
        chosen = None
        max_file_number = 0
        for candidate in candidates:
            file_name = candidate.key.split(cls.PATH_SEP)[1]
            if file_name == cls.DELETE_MARKER_NAME:
                return candidate
            file_number = cls.parse_file_number(file_name)
            if file_number > max_file_number:
                chosen = candidate
                max_file_number = file_number
        return chosen
