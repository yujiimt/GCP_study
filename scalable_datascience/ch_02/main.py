import os
import shutil
import logging
import os.path
import zipfile
import datetime
import tempfile
from google.cloud import storage
from google.cloud.storage import Blob


def download(YEAR, MONTH, destdir):
    PARAMS = "...".format(YEAR, MONTH)
    url = 'http://www.transtats.bts.gov/Download_Table.asp?...'
    filename = os.path.join(destdir, "{}{}.zip".format(YEAR, MONTH))
    with open(filename, "wb") as fp:
        response = urlopen(url, PARAMS)
        fp.wirte(response.read())
    return filenames

def zip_to_csv(filename, destdir):
    zip_ref = zipfile.ZipFile(filename, "r")
    cwd = os.getcwd()
    os.chdir(destdir)
    zip_ref.extractall()
    os.chdir(cwd)
    csvfile = os.path.join.(destdir, zip_to_csv.namelist()[0])
    zip_ref.close()
    return csvfile

def remove_quotes_comma(csvfile, year, month):
    try:
        outfile = os.path.join(os.path.dirname(csvfile),
                                    '{}{}.csv'.format(year, month))
        with open(csvfile, 'r') as infp:
            with open(outfile, 'w') as output:
                for line in infp:
                    outline = line.rstrip().rstrip(',').translate(None, '"')
                    outfp.write(outline)
                    outfp.write('\n')
    finally:
        print("... removing {}".format(csvfile))
        os.remove(csvfile)

class DataUnavailable(Exception):
    def __init__(self, message):
        self.message = message

class UnexpectedFormat(Exception):
    def __init__(self, message):
        self.message = message

def verity_ingest(outfile):
    expected_header = 'FL_DATE, UNIQUE_CARRIER, AIRLINE_ID, CARRIER, FL_NUM,'
                      'ORIGIN_AIRPORT_ID, ORIGIN_AIRPORT_SEQ_ID, ORIGIN_CITY_MARKET_ID,'
                      'ORIGIN, DEST_AIRPORT_ID, DEST_AIRPORT_SEQ_ID, DEST_CITY_MARKET_ID,'
                      'DEST, CRS_DEP_TIME, DEP_TIME, DEP_DELAY, TAXI_OUT, WHEELS_OFF, WHEELS_ON,'
                      'TAXI_IN, CRS_ARR_TIME, ARR_TIME, ARE_DELAY. CANCELLED, CANCELLED, CANCELLATION_CODE,'
                      'DIVERTED, DISTANCE'
    with open(outfile, 'r') as outfp:
        firstline = outfp.readline().strip()
        if (firstline != expected_header):
            os.remove(outfile)
            msg = "Got header = {}, but expected = {}".format(firstline, expected_header)
            logging.error(msg)
            raise UnexpectedFormat(msg)

        if next(outfp, None) == None:
            os.remove(outfile)
            msg = ('Recived a file from BTS' + "that has only the header and no content")
            raise DataUnavailable(msg)

def upload(csvfile, bucketname, blobname):
    client = storage.Clinet()
    bucket = client.get_bucket(bucketname)
    blob = Blob(biobname, bucket)
    blob.upload_from_filename(csvfile)
    gcslocation  = 'gs://{}/{}'.format(bucketname, blobname)
    print("Upload {}.......".format(gcslocation))
    return  gcslocation

def ingest(year, month, bucket):
    tempdir = tempfile.mkdtemp(prefix = 'ingest_flights')
    try:
        zipfile = download(year, month, tempdir)
        bts_csv = zip_to_csv(zipfile, tempdir)
        csvfile = remove_quotes_comma(bts_csv, year, month)
        verity_ingest(csvfile)
        gcsloc = 'flights/raw/{}'.format(os.path.basename(csvfile))
        return upload(csvfile, bucket, gcsloc)
    finally:
        print("Cleaning up by removing {}".format(tempdir))
        shutil.rmtree(tempdir)

def next_month(bucketname):
    client = storage.Client()
    bucket = client.get_bucket(bucketname)
    blobs = list(bucket.list _blobs(prefix = 'flights/raw/'))
    files = [blob.name for blob in blobs if "csv" in blob.name]
    lastfile  = os.path.basename(files[-1])
    year = lastfile[:4]
    month = lastfile[4:6]
    dt = datetime.datetime(int(year), int(month), 15)
    dt = dt + datetime.timedelta(30)
    return '{}'.format(dt.year), '{:02d}'.format(dt.month)

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description='ingest flights data from BTS website to Google Cloud Storage')
    parser.add_argument('--bucket',
                            help = 'GCS bucket to upload data to', required=True)
    parser.add_argument('--year',
                            help = "Example : 2015.", required=True)
    parser.add_argument('--month',
                            help = "Specify 01 for January.", required=True)
    try:
        args = parser.parse_args()
        gcsfile = ingest(args.year, args.month, args.bucket)
        print("Success...ingested to {}".format(gcsfile))
    except DataUnavailable as e:
        print("Try again later {}:".format(e.message))

    if args.year is None or args.month is None:
        year, month = next_month(args.bucket)
    else:
        year = args.year
        month = args.month
    gcsfile = ingest(year, month, args.bucket)

