import datetime
import logging
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


def get_data(contract_type, award_type, sess):
    dates = 'SIGNED_DATE:[2016/10/01,2016/10/03]'  # fix fields from October 01, 2016 to September 05, 2017
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
        tmp_obj = process_data(value['content'][contract_type], atom_type=contract_type)
        sess.query(DetachedAwardProcurement).\
            filter_by(detached_award_proc_unique=tmp_obj['detached_award_proc_unique']).\
            update({'base_and_all_options_value': tmp_obj['base_and_all_options_value'],
                    'base_exercised_options_val': tmp_obj['base_exercised_options_val'],
                    'updated_at': utcnow}, synchronize_session=False)
    sess.commit()


def process_data(data, atom_type):
    temp_obj = {}
    if atom_type == "award":
        temp_obj = award_id_values(data['awardID'], temp_obj)
    else:
        # transaction_number is a part of the unique identifier, set it to None
        temp_obj['transaction_number'] = None
        temp_obj = contract_id_values(data['contractID'], temp_obj)

    try:
        temp_obj['base_and_all_options_value'] = extract_text(data['dollarValues']['baseAndAllOptionsValue'])
    except:
        temp_obj['base_and_all_options_value'] = None

    try:
        temp_obj['base_exercised_options_val'] = extract_text(data['dollarValues']['baseAndExercisedOptionsValue'])
    except KeyError:
        temp_obj['base_exercised_options_val'] = None

    temp_obj['detached_award_proc_unique'] = generate_unique_string(temp_obj)
    return temp_obj


def main():
    sess = GlobalDB.db().session
    # IDV stuff
    award_types_idv = ["GWAC", "BOA", "BPA", "FSS", "IDC"]
    for award_type in award_types_idv:
        get_data("IDV", award_type, sess)
    # award stuff
    award_types_award = ["BPA Call", "Definitive Contract", "Purchase Order", "Delivery Order"]
    for award_type in award_types_award:
        get_data("award", award_type, sess)


if __name__ == '__main__':
    with create_app().app_context():
        configure_logging()
        main()
