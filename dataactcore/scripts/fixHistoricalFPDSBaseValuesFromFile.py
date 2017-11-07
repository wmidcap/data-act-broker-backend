import argparse
import datetime
import logging
import re
import os
import pandas as pd
import zipfile

from collections import OrderedDict

from dataactcore.config import CONFIG_BROKER
from dataactcore.interfaces.db import GlobalDB
from dataactcore.logging import configure_logging
from dataactcore.models.stagingModels import DetachedAwardProcurement
from dataactcore.scripts.pullFPDSData import generate_unique_string

from dataactvalidator.health_check import create_app

BLOCK_SIZE = 10000

logger = logging.getLogger(__name__)


def parse_fpds_file(f, sess, missing_rows, since_updated):
    utcnow = datetime.datetime.utcnow()
    logger.info("Starting file " + str(f))
    csv_file = 'datafeeds\\' + os.path.splitext(os.path.basename(f))[0]

    # count and print the number of rows
    nrows = 0
    with zipfile.ZipFile(f) as zfile:
        with zfile.open(csv_file) as dat_file:
            nrows = len(dat_file.readlines())
            logger.info("File contains %s rows", nrows)

    batches = nrows // BLOCK_SIZE
    last_block_size = (nrows % BLOCK_SIZE)
    batch, added_rows = 0, 0
    column_header_mapping = {
        "baseandexercisedoptionsvalue": 3,
        "baseandalloptionsvalue": 4,
        "agencyid": 94,
        "piid": 95,
        "modnumber": 96,
        "transactionnumber": 97,
        "idvagencyid": 99,
        "idvpiid": 100
    }
    column_header_mapping_ordered = OrderedDict(sorted(column_header_mapping.items(), key=lambda c: c[1]))
    model_mapping = {
        "baseandexercisedoptionsvalue": "base_exercised_options_val",
        "baseandalloptionsvalue": "base_and_all_options_value",
        "agencyid": "agency_id",
        "piid": "piid",
        "modnumber": "award_modification_amendme",
        "transactionnumber": "transaction_number",
        "idvagencyid": "referenced_idv_agency_iden",
        "idvpiid": "parent_award_id"
    }

    while batch <= batches:
        skiprows = 1 if batch == 0 else (batch * BLOCK_SIZE)
        nrows = (((batch + 1) * BLOCK_SIZE) - skiprows) if (batch < batches) else last_block_size
        logger.info('Starting load for rows %s to %s', skiprows + 1, nrows + skiprows)

        with zipfile.ZipFile(f) as zfile:
            with zfile.open(csv_file) as dat_file:
                # retrieve rows from the file
                data = pd.read_csv(dat_file, dtype=str, header=None, skiprows=skiprows, nrows=nrows,
                                   usecols=column_header_mapping_ordered.values(),
                                   names=column_header_mapping_ordered.keys())
                data = data.where((pd.notnull(data)), None)

                # update rows in the database
                logger.info("Updating {} rows".format(len(data.index)))
                for index, row in data.iterrows():
                    # create new object with correct values
                    tmp_obj = {}
                    for key in [key for key in model_mapping]:
                        tmp_obj[model_mapping[key]] = str(row[key]) if row[key] is not None else None

                    # generate unique string
                    tmp_obj['detached_award_proc_unique'] = generate_unique_string(tmp_obj)

                    # retrieve the row from the database
                    record = sess.query(DetachedAwardProcurement).\
                                  filter_by(detached_award_proc_unique=tmp_obj['detached_award_proc_unique']).first()
                    if record is None:
                        # add data to array to be printed later
                        missing_rows.append(tmp_obj)
                    elif record.updated_at >= datetime.date(2017, 9, 5):
                        # log the unique key and add data to array to be printed later
                        logger.info("Skipping record due to updated_at field: %s",
                                    str(tmp_obj['detached_award_proc_unique']))
                        since_updated.append(record)
                    else:
                        # update record
                        record['base_and_all_options_value'] = tmp_obj['base_and_all_options_value']
                        record['base_exercised_options_val'] = tmp_obj['base_exercised_options_val']
                        record['updated_at'] = utcnow
                # commit changes
                sess.commit()
        added_rows += nrows
        batch += 1
    logger.info("Finished loading file")


def main():
    sess = GlobalDB.db().session
    parser = argparse.ArgumentParser(description='Pull data from the FPDS Atom Feed.')
    parser.add_argument('-p', '--path', help='Filepath to the directory to pull the files from.', nargs=1, type=str)
    args = parser.parse_args()
    missing_rows, since_updated = [], []

    # use filepath if provided, otherwise use the default
    file_path = args.path[0] if args.path else os.path.join(CONFIG_BROKER["path"], "dataactvalidator", "config", "fabs")

    # parse all Contracts files
    file_list = [f for f in os.listdir(file_path)]
    for file in file_list:
        if re.match('^\d{4}_All_Contracts_Full_\d{8}.csv.zip', file):
            parse_fpds_file(open(os.path.join(file_path, file)).name, sess, missing_rows, since_updated)

    if len(missing_rows) > 0:
        logger.info('Records that don\'t exist in the database:')
        for row in missing_rows:
            logger.info('unique_key: %s, base_exercised_options_val: %s, base_and_all_options_value: %s',
                        row['detached_award_proc_unique'], row['base_exercised_options_val'],
                        row['base_and_all_options_value'])
    if len(since_updated) > 0:
        logger.info('Records updated since 09/05/2017 (and not updated from this script):')
        for row in since_updated:
            logger.info('unique_key: %s, base_exercised_options_val: %s, base_and_all_options_value: %s, updated_at: %s',
                        row['detached_award_proc_unique'], row['base_exercised_options_val'],
                        row['base_and_all_options_value'], row['updated_at'])

if __name__ == '__main__':
    with create_app().app_context():
        configure_logging()
        main()
