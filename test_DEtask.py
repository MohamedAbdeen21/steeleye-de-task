from warnings import simplefilter
from os import remove, rmdir
from os.path import exists
from boto3 import client
import unittest
from requests.exceptions import ConnectionError, HTTPError
from zipfile import BadZipFile

from DEtask import extractToCSV, downloadZipAndRead, extractZipUrl, uploadCSVtoS3

class TestExtractZipUrl(unittest.TestCase):
    base_url = "https://registers.esma.europa.eu/solr/esma_registers_firds_files/select?q=*&fq=publication_date:%5B2021-01-17T00:00:00Z+TO+2021-01-19T23:59:59Z%5D&wt=xml&indent=true&start=0&rows=100"
    def test_index(self):
        # Test when the index is invalid
        with self.assertRaises(IndexError, msg="should raise an error on invalid index"):
            extractZipUrl(self.base_url, 5)
    def test_url(self):
        # Test when the URL is invalid
        with self.assertRaises(ConnectionError, msg="should raise an error invalid domain"):
            extractZipUrl(self.base_url.replace('esma','esmax'), 0)
        with self.assertRaises(HTTPError, msg="Should raise an error on status_code of family 400 or 500"):
            extractZipUrl(self.base_url + "z", 0)

    def test_working(self):
        # Test when the index and URL are correct
        self.assertEqual(extractZipUrl(self.base_url,0), "http://firds.esma.europa.eu/firds/DLTINS_20210117_01of01.zip", "function is not correct")
        self.assertEqual(extractZipUrl(self.base_url, 2), "http://firds.esma.europa.eu/firds/DLTINS_20210119_02of02.zip", "function is not correct")
        self.assertEqual(extractZipUrl(self.base_url, -2),  "http://firds.esma.europa.eu/firds/DLTINS_20210119_02of02.zip", "fails on negative index")

class TestDownloadZipAndRead(unittest.TestCase):
    base_url = "http://firds.esma.europa.eu/firds/DLTINS_20210117_01of01.zip"
    def test_url(self):
        # Test when the URL is invalid
        with self.assertRaises(ConnectionError, msg="should raise an error on invalid domain"):
            downloadZipAndRead(self.base_url.replace('esma','esmax'))
        with self.assertRaises(BadZipFile, msg="Fail on things that are not a zipfiles"):
            downloadZipAndRead(self.base_url.replace("20210117","19800101"))
        with self.assertRaises(BadZipFile, msg="should raise an error on status_code of family 400 or 500"):
            downloadZipAndRead(self.base_url + 'z')


class TestExtractToCSV(unittest.TestCase):
    base_url = "http://firds.esma.europa.eu/firds/DLTINS_20210117_01of01.zip"
    def test_csv_write(self):
        # Works as expected
        extractToCSV(downloadZipAndRead(self.base_url)[:2], csv_path='./data/temp.csv')
        with open('./data/temp.csv') as file:
            self.assertEqual(''.join(file.readlines()), ''.join(["FinInstrmGnlAttrbts.Id,FinInstrmGnlAttrbts.FullNm,FinInstrmGnlAttrbts.ClssfctnTp,FinInstrmGnlAttrbts.CmmdtyDerivInd,FinInstrmGnlAttrbts.NtnlCcy,Issr\n",
                                                            "DE000A1R07V3,Kreditanst.f.Wiederaufbau     Anl.v.2014 (2021),DBFTFB,false,EUR,549300GDPG70E3MBBU98\n",
                                                            "DE000A1R07V3,KFW 1 5/8 01/15/21,DBFTFB,false,EUR,549300GDPG70E3MBBU98\n"]),
                                                            msg="csv output mismatch")
            remove('./data/temp.csv')
        
    def test_create_dir(self):
        # Create directory when given a directory that doesn't exist
        extractToCSV([], csv_path='./data/tempdir/temp.csv')
        self.assertTrue(exists('./data/tempdir/temp.csv'), msg="doesn't create directory if it doesn't exist")
        remove('./data/tempdir/temp.csv')
        rmdir('./data/tempdir/')

class TestUploadCSVtoS3(unittest.TestCase):
    bucket = 'steeleye-de-task-bucket'
    def test_connection(self):
        simplefilter("ignore", ResourceWarning)
        # Test the connection to 'steeleye-de-task-bucket' by uploading a dummy file
        open('./data/dummy-file.csv', 'w').close()
        uploadCSVtoS3(self.bucket,'./data/dummy-file.csv', 'me-central-1', object_name='test.csv')

        s3 = client('s3')
        object = s3.get_object(Bucket=self.bucket, Key="test.csv")
        data = object['Body'].read().decode('utf-8')

        s3.delete_object(Bucket=self.bucket, Key="test.csv")
        remove('./data/dummy-file.csv')
        self.assertEqual(data, '', msg="expects to find an empty file in bucket")
    
    def test_only_csv(self):
        simplefilter("ignore", ResourceWarning)
        # Test that the function only accepts files with .csv extension
        open('./data/dummy-file.txt','w').close()
        with self.assertRaises(FileNotFoundError, msg='expects to throw an error on wrong file format'):
            uploadCSVtoS3(self.bucket, './data/dummy-file.txt', 'me-central-1', object_name = 'test.txt')
        remove('./data/dummy-file.txt')
