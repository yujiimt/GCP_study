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