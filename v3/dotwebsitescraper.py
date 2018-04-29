""" Download and save permit data from NYC DOT
This executable is used to download DOT Permits for street work from nycstreets.net:

The DOT server does not seem to allow querying of past or completed permits, so this script is run daily to pull down
and save the data. The key-value data store is saved in Google Cloud Datastore and the PDF's are also saved.
Example usage:
    ./dotpermitscraper \
        --url=https://nycstreets.net/Public/Permit/SearchPermits \
        --googleproject=nycstreets-200919
"""

import argparse
from argparse import RawTextHelpFormatter
import requests
from google.cloud import datastore
import datetime
import os
from google.cloud import storage
import six
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="./NYCStreets-cea4c7adde3d.json"

def get_storage_client():
    return storage.Client()

def upload_file(permitnumber, filename, content_type):
    try:
        client = get_storage_client()
        bucket = client.bucket("nycstreets")
        blob = bucket.blob("/permitpdf/"+permitnumber+".pdf")

        blob.upload_from_filename(
            filename)

        url = blob.public_url

        if isinstance(url, six.binary_type):
            url = url.decode('utf-8')

        return url
    except TypeError:
        return False


def create_client(project_id):
    return datastore.Client(project_id)


def permitnumberInDB(client,permitnumber):
    query = client.query(kind='Permit')
    query.add_filter('PermitNumber', '=', permitnumber)
    results = list(query.fetch())
    if len(results)>0:
        return True
    else:
        return False

def permitpdfSavedInDB(client,permitnumber):
    query = client.query(kind='Permit')
    query.add_filter('PermitNumber', '=', permitnumber)
    results = list(query.fetch())
    if (len(results)>0):
        if "permitpdf" in results[0]:
            return True
        else:
            return False

def savePDF(client, permitnumber):
    url = "https://nycstreets.net/Public/Document/ViewPermitPDF/?id=" + permitnumber
    response = requests.get(url, headers={'referer': "https://nycstreets.net"})


    if response.status_code == 200:
        with open('/tmp/test.pdf', 'wb') as f:
            f.write(response.content)
        if (os.path.getsize('/tmp/test.pdf')>0):
            if upload_file(permitnumber,"/tmp/test.pdf","application/pdf"):
                updatePDFSave(client, permitnumber, permitnumber + ".pdf")

def updatePDFSave(client,permitnumber, filename):
    query = client.query(kind='Permit')
    query.add_filter('PermitNumber', '=', permitnumber)
    results = list(query.fetch())
    if len(results) > 0:
        dotpermit = results[0]
        dotpermit['permitpdf'] = filename

        client.put(dotpermit)
        print("Updating PDF URL")

def updateTimeLastSeen(client, permitnumber):
    query = client.query(kind='Permit')
    query.add_filter('PermitNumber', '=', permitnumber)
    results = list(query.fetch())
    print(str(results))
    if len(results) > 0:
        dotpermit= results[0]
        dotpermit['lastseen'] = datetime.date.today().strftime("%m/%d/%Y")

        client.put(dotpermit)
        print("Updating Time Last Seen")

def putjsonitemingooglecloud(client, jsonitem):

    for jsonobject in jsonitem:
        print("Json object is "+str(jsonobject))
        key = client.key('Permit')

        task = datastore.Entity(key, exclude_from_indexes=['Wkt'])
        dictionarytosave = jsonobject
        permitnumber = dictionarytosave["PermitNumber"]
        if not permitnumberInDB(client,permitnumber):
            dictionarytosave["datecreated"] = datetime.date.today().strftime("%m/%d/%Y")
            dictionarytosave["lastseen"] = datetime.date.today().strftime("%m/%d/%Y")
            task.update(dictionarytosave)
            client.put(task)
            print("Saving")

        else:
            updateTimeLastSeen(client,permitnumber)
            print("Not Saving, Already exists")

        if not permitpdfSavedInDB(client,permitnumber):
            savePDF(client,permitnumber)

def getLastRanDate(client):
    query = client.query(kind='Permit')
    query.order = ["-datecreated"]
    results = list(query.fetch(limit=1))
    lastRan = results[0]["datecreated"]
    print("Last Date is  "+lastRan)
    return lastRan

def getTomorrow():
    tomorrow = datetime.date.today() + datetime.timedelta(days=1)
    return tomorrow.strftime('%m/%d/%Y')

def queryUrlSavePermits(client,url,pagenumber):
    print("Querying with page "+str(pagenumber))
    lastRanDate = getLastRanDate(client)
    tomorrow = getTomorrow()
    payload = {'PermitIssueDateFrom': lastRanDate, 'PermitIssueDateTo': tomorrow, 'page': pagenumber, 'rows': 250,
               'sidx': 'PermitIssueDateFrom', 'sord': 'desc', 'LocationSearchType': '0', '_': '1523557546750'}
    r = requests.get(url, params=payload)
    jsonreturn = r.json()
    pages = jsonreturn["TotalPages"]
    if "PermitList" in jsonreturn:
        putjsonitemingooglecloud(client, jsonreturn["PermitList"])

    if pagenumber == 1:
        for i in range(2,pages+1):
            queryUrlSavePermits(client,url,i)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Download images every second from dotsignals.org', formatter_class=RawTextHelpFormatter)
    parser.add_argument('-url', help='the url for the permits you want to download')
    parser.add_argument('-googleproject', help='name of google cloud project')
    args = parser.parse_args()
    client = create_client(args.googleproject)
    queryUrlSavePermits(client, args.url, 1)



