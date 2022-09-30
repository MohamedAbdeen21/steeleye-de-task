from io import BytesIO
from zipfile import ZipFile
from xml.dom.minidom import parse, parseString, Element
import logging
import requests
import csv
import os
import boto3

logging.basicConfig(
    level=logging.INFO,
    filename="./data/logs.log",
    filemode="a",
    format="%(levelname)s: %(asctime)s - %(funcName)s - %(message)s",
)


def extractZipUrl(url: str, index: int = 0) -> str:
    """
    Extracts the i-th .zip link from the given url

    Parameters
    ----------
    url : str
        The url to look for .zip links in.
    index : int , optional
        The i-th .zip link, default 0.

    Returns
    -------
    str
        the URL of the i-th .zip link.

    Raises
    ------
    IndexError
        If the index specified is out of bounds.
    requests.exceptions.HTTPError
        If status code is of family 400 or 500.
        (bad request, forbidden, intenral server error, ..)
    """
    try:
        xml_file = requests.get(url)

        if xml_file.status_code >= 400:
            logging.error(f"Can't download URL, status code = {xml_file.status_code}")
            raise requests.exceptions.HTTPError(f"status code = {xml_file.status_code}")

        logging.info("Downloaded URL. Parsing for ZIP ...")
        parsed_xml = parseString(xml_file.text).documentElement
        documents = parsed_xml.getElementsByTagName("doc")
        link_element = documents[index].childNodes[3]
        link = link_element.firstChild.nodeValue
        logging.info(f"Found ZIP link: {link}")
        return link
    except IndexError as e:
        logging.exception(f"Failed to find valid ZIP link for index {index}")
        raise e


def downloadZipAndRead(url: str) -> list[Element]:
    """
    Download the given zip file and extracts the
    'FinInstrm' tag.

    Parameters
    ----------
    url: str
        The URL of the .zip file to download and parse.

    Returns
    -------
    list[Element]
        A list of xml.dom.minidom.Element type of the
        'FinInstrm' tags.
    """
    zip_file = requests.get(url)
    unzipped_files = ZipFile(BytesIO(zip_file.content))
    unzipped_file = unzipped_files.namelist()[0]
    parsed_data = parse(unzipped_files.open(unzipped_file)).documentElement

    data_items = parsed_data.getElementsByTagName("FinInstrm")
    logging.info(f"found {len(data_items)} elements")
    return data_items


def extractToCSV(elements: list[Element], csv_path: str) -> None:
    """
    Parse a list of xml.dom.minidom.Elements type for
    the required tags, then write to specified .csv path with
    pre-defined headers

    Parameters
    ----------
    elements: list[Element]
        a list of xml.dom.minidom.Element type to be parsed
        for the required tags; extracted from downloadZipAndRead
        function.

    Returns
    -------
    None
    """

    headers = [
        [
            "FinInstrmGnlAttrbts.Id",
            "FinInstrmGnlAttrbts.FullNm",
            "FinInstrmGnlAttrbts.ClssfctnTp",
            "FinInstrmGnlAttrbts.CmmdtyDerivInd",
            "FinInstrmGnlAttrbts.NtnlCcy",
            "Issr",
        ]
    ]

    # Issr is not included because it is not in the same tag as these fields.
    # We add it explicitly after extracting these fields.
    tags = ["Id", "FullNm", "ClssfctnTp", "CmmdtyDerivInd", "NtnlCcy"]

    rows = []
    for element in elements:
        row = []
        info = element.firstChild.firstChild

        for tag in tags:
            row.append(info.getElementsByTagName(tag)[0].firstChild.nodeValue)

        row.append(element.getElementsByTagName("Issr")[0].firstChild.nodeValue)
        rows.append(row)
    logging.info("Successfully parsed data, Writing to CSV ...")

    if not os.path.exists(os.path.dirname(csv_path)):
        os.mkdir(os.path.dirname(csv_path))
        logging.info(f"Created directory {csv_path}")

    with open(csv_path, "w") as handle:
        writer = csv.writer(handle)
        writer.writerows(headers + rows)

    logging.info(f"Successfully created CSV in directory {csv_path}")

    return None


def uploadCSVtoS3(bucket: str, csv_path: str, region: str, object_name: str) -> None:
    """
    Upload the CSV file to S3 Bucket

    Parameters
    ----------
    bucket: str
        The name of the S3 bucket to upload to
    csv_path: str
        The path of the .csv file to be uploaded
    region: str
        The AWS region where the S3 bucket is
    object_name: str
        The name of the object on S3 after being uploaded

    Returns
    -------
    None

    Raises
    ------
    FileNotFoundError
        If the provided csv_path is invalid
    """
    if ".csv" not in csv_path or not os.path.exists(csv_path):
        raise FileNotFoundError
    logging.info(f"Uploading to bucket {bucket}")
    s3 = boto3.client("s3", region_name=region)
    s3.upload_file(csv_path, bucket, object_name)


def main():
    """
    Run the functions with hard-coded arguments
    """
    target_url = "https://registers.esma.europa.eu/solr/esma_registers_firds_files/select?q=*&fq=publication_date:%5B2021-01-17T00:00:00Z+TO+2021-01-19T23:59:59Z%5D&wt=xml&indent=true&start=0&rows=100"
    csv_path = "/tmp/data.csv"
    # already created and have public read access
    s3_bucket = "steeleye-de-task-bucket"
    s3_object_name = "data.csv"
    aws_region = "me-central-1"

    extractToCSV(downloadZipAndRead(extractZipUrl(target_url)), csv_path=csv_path)
    uploadCSVtoS3(s3_bucket, csv_path, aws_region, s3_object_name)

    logging.info("Terminated successfully")


if __name__ == "__main__":
    main()
