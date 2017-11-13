import argparse
import logging
import sqlalchemy

from dataactcore.interfaces.db import GlobalDB, _DB
from dataactcore.logging import configure_logging
from dataactcore.models.stagingModels import DetachedAwardProcurement

from dataactvalidator.health_check import create_app

from sqlalchemy import cast, Date
from sqlalchemy.orm import sessionmaker, scoped_session

logger = logging.getLogger(__name__)

QUERY_SIZE = 10000


def update_dollar_values(sess, external_sess):
    # save code space by using dap_model instead of DetachedAwardProcurement
    dap_model = DetachedAwardProcurement

    logger.info('Starting to load all data')
    page_idx = 0
    while True:
        # pull data, QUERY_SIZE at a time
        page_start = QUERY_SIZE * page_idx
        page_stop = QUERY_SIZE * (page_idx + 1)
        logger.info('Querying 10,000 records')
        updated_data = external_sess.query(dap_model.detached_award_proc_unique, dap_model.base_and_all_options_value,
                                           dap_model.base_exercised_options_val, dap_model.federal_action_obligation,
                                           dap_model.potential_total_value_awar, dap_model.current_total_value_award,
                                           dap_model.total_obligated_amount).\
            filter(cast(dap_model.action_date, Date) >= '10/01/2015').\
            filter(cast(dap_model.action_date, Date) <= '10/01/2015').\
            slice(page_start, page_stop).all()

        # no more records to pull
        if updated_data is None:
            break

        logger.info('Updating records %s-%s', page_start, page_stop + len(updated_data))
        # update all the records in the DB
        for record in updated_data:
            sess.query(dap_model).filter_by(detached_award_proc_unique=record.detached_award_proc_unique).update({
                "base_and_all_options_value": record.base_and_all_options_value,
                "base_exercised_options_val": record.base_exercised_options_val,
                "federal_action_obligation": record.federal_action_obligation,
                "potential_total_value_awar": record.potential_total_value_awar,
                "current_total_value_award": record.current_total_value_award,
                "total_obligated_amount": record.total_obligated_amount
            })

        # ensure we're not done, then increment the page_idx
        if len(updated_data) < QUERY_SIZE:
            break
        page_idx += 1

    logger.info('Finished updating all data')


def main():
    sess = GlobalDB.db().session

    parser = argparse.ArgumentParser(description='Pull data from the FPDS Atom Feed.')
    parser.add_argument('-t', '--host', help='External DB host IP address', nargs=1, type=str)
    parser.add_argument('-n', '--dbname', help='External DB name', nargs=1, type=str)
    parser.add_argument('-u', '--username', help='External DB username', nargs=1, type=str)
    parser.add_argument('-p', '--password', help='External DB password', nargs=1, type=str)
    args = parser.parse_args()

    host = args.host[0]
    dbname = args.dbname[0]
    username = args.username[0]
    password = args.password[0]
    uri = "postgresql://{}:{}@{}:5432/{}".format(username, password, host, dbname)

    engine = sqlalchemy.create_engine(uri, pool_size=100, max_overflow=50)
    connection = engine.connect()
    scoped_session_maker = scoped_session(sessionmaker(bind=engine))
    external_sess = _DB(engine, connection, scoped_session_maker, scoped_session_maker()).session

    update_dollar_values(sess, external_sess)


if __name__ == '__main__':
    with create_app().app_context():
        configure_logging()
        main()
