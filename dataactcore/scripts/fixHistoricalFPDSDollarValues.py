import argparse
import csv
import datetime
import logging
import re
import requests
import time
import xmltodict

from dataactcore.interfaces.db import GlobalDB
from dataactcore.logging import configure_logging
from dataactcore.scripts.pullFPDSData import extract_text, list_data
from dataactcore.utils.responseException import ResponseException
from dataactcore.utils.statusCode import StatusCode

from dataactvalidator.health_check import create_app

FEED_URL = "https://www.fpds.gov/ezsearch/FEEDS/ATOM?FEEDNAME=PUBLIC&templateName=1.4.5&q="
NAMESPACES = {
    'http://www.fpdsng.com/FPDS': None, 'http://www.w3.org/2005/Atom': None, 'https://www.fpds.gov/FPDS': None
}

logger = logging.getLogger(__name__)


def get_data(contract_type, award_type, sess, start, end, filename):
    start_date = start if start else '2015/10/01'
    end_date = end if end else '2017/09/05'
    utcnow = datetime.datetime.utcnow()
    dates = 'SIGNED_DATE:[{},{}]'.format(start_date, end_date)
    feed_string = '{}{} CONTRACT_TYPE:"{}" AWARD_TYPE:"{}"'.format(FEED_URL, dates, contract_type.upper(), award_type)
    i, loops = 0, 0
    logger.info('Starting get feed: %s', feed_string)

    with open(filename, 'w', newline='') as csv_file:
        out_csv = csv.writer(csv_file, delimiter=',', quoting=csv.QUOTE_MINIMAL, lineterminator='\n')

        # write headers
        out_csv.writerow(['modNumber', 'transactionNumber', 'PIID', 'agencyID', 'agencyID', 'modNumber', 'PIID',
                          'baseAndAllOptionsValue', 'baseAndExercisedOptionsValue', 'totalBaseAndAllOptionsValue',
                          'totalBaseAndExercisedOptionsValue', 'totalObligatedAmount'])
        while True:
            loops += 1
            exception_retries = -1
            retry_sleep_times = [5, 30, 60, 180, 300]
            # looping in case feed breaks
            while True:
                try:
                    resp = requests.get('{}&start={}'.format(feed_string, str(i)), timeout=60)
                    resp_data = xmltodict.parse(resp.text, process_namespaces=True, namespaces=NAMESPACES)
                    break
                except ConnectionResetError:
                    exception_retries += 1
                    # retry up to 3 times before raising an error
                    if exception_retries < len(retry_sleep_times):
                        time.sleep(retry_sleep_times[exception_retries])
                    else:
                        raise ResponseException(
                            "Connection to FPDS feed lost, maximum retry attempts exceeded.", StatusCode.INTERNAL_ERROR
                        )

            # only list the data if there's data to list
            try:
                listed_data = list_data(resp_data['feed']['entry'])
            except KeyError:
                listed_data = []

            for item in listed_data:
                out_csv.writerow(process_data(item['content'][contract_type], atom_type=contract_type))
                i += 1

            if loops % 100 == 0 and loops != 0:
                logger.info("Retrieved %s lines of get %s: %s feed", i, contract_type, award_type)

            # if we got less than 10 records, we can stop calling the feed
            if len(listed_data) < 10:
                break

    logger.info("Total entries in %s: %s feed: " + str(i), contract_type, award_type)
    logger.info("records per second: %s", str(i / (datetime.datetime.utcnow()-utcnow).total_seconds()))


def process_data(data, atom_type):
    row = []

    # set strings for pulling data from the atom fields
    if atom_type == 'award':
        atom_type_string = 'awardID'
        atom_type_substring = 'awardContractID'
    else:
        atom_type_string = 'contractID'
        atom_type_substring = 'IDVID'

    # for all values, assign them if they exist, otherwise set to None
    # retrieve the fields within the unique identifier string
    unique_atom_fields = ['modNumber', 'transactionNumber', 'PIID', 'agencyID']
    referenced_idv_fields = ['agencyID', 'modNumber', 'PIID']
    for value in unique_atom_fields:
        try:
            row.append(extract_text(data[atom_type_string][atom_type_substring][value]))
        except:
            row.append(None)
    for value in referenced_idv_fields:
        try:
            row.append(extract_text(data[atom_type_string]['referencedIDVID'][value]))
        except:
            row.append(None)

    # retrieve the dollarValues
    dollar_values = ['baseAndAllOptionsValue', 'baseAndExercisedOptionsValue']
    for value in dollar_values:
        try:
            row.append(extract_text(data['dollarValues'][value]))
        except:
            row.append(None)

    # retrieve the totalDollarValues
    total_dollar_values = ['totalBaseAndAllOptionsValue', 'totalBaseAndExercisedOptionsValue', 'totalObligatedAmount']
    for value in total_dollar_values:
        try:
            row.append(extract_text(data['totalDollarValues'][value]))
        except:
            row.append(None)

    return row


def main():
    sess = GlobalDB.db().session
    parser = argparse.ArgumentParser(description='Pull data from the FPDS Atom Feed.')
    parser.add_argument('-s', '--start', help='First date in the pull', nargs=1, type=str)
    parser.add_argument('-e', '--end', help='Last date in the pull', nargs=1, type=str)
    parser.add_argument('-t', '--types', help='Update values for just the award types listed', nargs="+", type=str)
    parser.add_argument('-f', '--filename', help='Path to file to write to', nargs=1, type=str)
    args = parser.parse_args()

    # add start date, end date, and/or requested types if they exist
    start_date = args.start[0] if args.start else None
    end_date = args.end[0] if args.end else None
    requested_types = args.types if args.types else ["GWAC", "BOA", "BPA", "FSS", "IDC", "BPA Call",
                                                     "Definitive Contract", "Purchase Order", "Delivery Order"]
    filename = args.filename if args.filename[0] else 'fpds_dollar_values.csv'

    # ensure start and end dates are in the correct format
    regex = re.compile('[0-9][0-9][0-9][0-9]/[0-9][0-9]/[0-9][0-9]')
    if start_date and not regex.match(start_date):
        logger.warning('Start date is not in the proper format')
        return
    if end_date and not regex.match(end_date):
        logger.warning('End date is not in the proper format')
        return

    # loop through "IDV" award types with the selected dates
    award_types_idv = ["GWAC", "BOA", "BPA", "FSS", "IDC"]
    for award_type in award_types_idv:
        if award_type in requested_types:
            get_data("IDV", award_type, sess, start_date, end_date, filename)

    # loop through "award" award types with the selected dates
    award_types_award = ["BPA Call", "Definitive Contract", "Purchase Order", "Delivery Order"]
    for award_type in award_types_award:
        if award_type in requested_types:
            get_data("award", award_type, sess, start_date, end_date, filename)


if __name__ == '__main__':
    with create_app().app_context():
        configure_logging()
        main()
