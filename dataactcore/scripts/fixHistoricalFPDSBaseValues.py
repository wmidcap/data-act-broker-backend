import argparse
import datetime
import logging
import re
import requests
import time
import xmltodict

from dataactcore.interfaces.db import GlobalDB
from dataactcore.logging import configure_logging
from dataactcore.models.stagingModels import DetachedAwardProcurement
from dataactcore.scripts.pullFPDSData import (award_id_values, contract_id_values, extract_text, generate_unique_string,
                                              list_data)
from dataactcore.utils.responseException import ResponseException
from dataactcore.utils.statusCode import StatusCode

from dataactvalidator.health_check import create_app

FEED_URL = "https://www.fpds.gov/ezsearch/FEEDS/ATOM?FEEDNAME=PUBLIC&templateName=1.4.5&q="
NAMESPACES = {
    'http://www.fpdsng.com/FPDS': None, 'http://www.w3.org/2005/Atom': None, 'https://www.fpds.gov/FPDS': None
}

logger = logging.getLogger(__name__)


def get_data(contract_type, award_type, sess, start_date='2016/10/01', end_date='2017/09/05'):
    dates = 'SIGNED_DATE:[{},{}]'.format(start_date, end_date)
    feed_string = '{}{} CONTRACT_TYPE:"{}" AWARD_TYPE:"{}"'.format(FEED_URL, dates, contract_type.upper(), award_type)
    data = []
    i, loops = 0, 0
    logger.info('Starting get feed: %s', feed_string)
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

        for ld in listed_data:
            data.append(ld)
            i += 1

        # Log which one we're on so we can keep track of how far we are, insert into DB every 5k lines
        if loops % 500 == 0 and loops != 0:
            logger.info("Retrieved %s lines of get %s: %s feed, writing next 5,000 to DB", i, contract_type, award_type)
            process_and_add(data, contract_type, sess)
            data = []

            logger.info("Successfully inserted 5,000 lines of get %s: %s feed, continuing feed retrieval",
                        contract_type, award_type)

        # if we got less than 10 records, we can stop calling the feed
        if len(listed_data) < 10:
            break

    logger.info("Total entries in %s: %s feed: " + str(i), contract_type, award_type)

    if data != []:
        # insert whatever is left
        logger.info("Processing remaining lines for %s: %s feed", contract_type, award_type)
        process_and_add(data, contract_type, sess)


def process_and_add(data, contract_type, sess):
    utcnow = datetime.datetime.utcnow()
    for value in data:
        # retrieve necessary data from the FPDS object
        tmp_obj = process_data(value['content'][contract_type], atom_type=contract_type)
        # update the database with the new content
        print(tmp_obj['detached_award_proc_unique'])
        thing = sess.query(DetachedAwardProcurement).\
            filter_by(detached_award_proc_unique=tmp_obj['detached_award_proc_unique']).\
            update({'base_and_all_options_value': tmp_obj['base_and_all_options_value'],
                      'base_exercised_options_val': tmp_obj['base_exercised_options_val'],
                      'updated_at': utcnow}, synchronize_session=False)
    sess.commit()


def process_data(data, atom_type):
    temp_obj = {}
    # retrieve the fields within the unique identifier string 
    if atom_type == "award":
        temp_obj = award_id_values(data['awardID'], temp_obj)
    else:
        # transaction_number is a part of the unique identifier, set it to None
        temp_obj['transaction_number'] = None
        temp_obj = contract_id_values(data['contractID'], temp_obj)

    # assign values if they exist, otherwise set to None
    try:
        temp_obj['base_and_all_options_value'] = extract_text(data['dollarValues']['baseAndAllOptionsValue'])
    except:
        temp_obj['base_and_all_options_value'] = None
    try:
        temp_obj['base_exercised_options_val'] = extract_text(data['dollarValues']['baseAndExercisedOptionsValue'])
    except KeyError:
        temp_obj['base_exercised_options_val'] = None

    # generate the unique identifier string
    temp_obj['detached_award_proc_unique'] = generate_unique_string(temp_obj)
    return temp_obj


def main():
    sess = GlobalDB.db().session
    parser = argparse.ArgumentParser(description='Pull data from the FPDS Atom Feed.')
    parser.add_argument('-s', '--start', help='First date in the pull', nargs=1, type=str)
    parser.add_argument('-e', '--end', help='Last date in the pull', nargs=1, type=str)
    parser.add_argument('-t', '--types', help='Update values for just the award types listed', nargs="+", type=str)
    args = parser.parse_args()

    # add start date and/or end date if they exist
    init_params = [sess]
    start_date, end_date = None, None
    if args.start:
        start_date = args.start[0]
        init_params.append(start_date)
    if args.end:
        end_date = args.end[0]
        init_params.append(end_date)

    # set requested types if any exist
    requested_types = args.types if args.types else ["GWAC", "BOA", "BPA", "FSS", "IDC", "BPA Call",
                                                     "Definitive Contract", "Purchase Order", "Delivery Order"]

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
            params = ["IDV", award_type] + init_params
            get_data(*params)

    # loop through "award" award types with the selected dates
    award_types_award = ["BPA Call", "Definitive Contract", "Purchase Order", "Delivery Order"]
    for award_type in award_types_award:
        if award_type in requested_types:
            params = ["award", award_type] + init_params
            get_data(*params)


if __name__ == '__main__':
    with create_app().app_context():
        configure_logging()
        main()
