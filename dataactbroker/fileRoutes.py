from datetime import datetime
from functools import wraps
from flask import request, g
from sqlalchemy import desc
from webargs import fields as webargs_fields, validate as webargs_validate
from webargs.flaskparser import parser as webargs_parser, use_kwargs

from dataactbroker.handlers.fileHandler import (
    FileHandler, get_error_metrics, get_status, list_submissions as list_submissions_handler,
    narratives_for_submission, submission_report_url, update_narratives, list_certifications, file_history_url)
from dataactbroker.handlers.submission_handler import delete_all_submission_data, get_submission_stats

from dataactbroker.permissions import requires_login, requires_submission_perms

from dataactcore.interfaces.db import GlobalDB
from dataactcore.interfaces.function_bag import get_fabs_meta

from dataactcore.models.lookups import FILE_TYPE_DICT, FILE_TYPE_DICT_LETTER, PUBLISH_STATUS_DICT
from dataactcore.models.jobModels import Submission, Job, CertifyHistory, SubmissionWindow

from dataactcore.utils.jsonResponse import JsonResponse
from dataactcore.utils.responseException import ResponseException
from dataactcore.utils.statusCode import StatusCode


# Add the file submission route
def add_file_routes(app, create_credentials, is_local, server_path):
    """ Create routes related to file submission for flask app """

    # Keys for the post route will correspond to the four types of files
    @app.route("/v1/submit_files/", methods=["POST"])
    @requires_login
    def submit_files():
        file_manager = FileHandler(request, is_local=is_local, server_path=server_path)

        sess = GlobalDB.db().session

        start_date = request.json.get('reporting_period_start_date')
        end_date = request.json.get('reporting_period_end_date')
        is_quarter = request.json.get('is_quarter_format', False)

        if not (start_date is None or end_date is None):
            formatted_start_date, formatted_end_date = FileHandler.check_submission_dates(start_date,
                                                                                          end_date, is_quarter)

            submissions = sess.query(Submission).filter(
                Submission.cgac_code == request.json.get('cgac_code'),
                Submission.frec_code == request.json.get('frec_code'),
                Submission.reporting_start_date == formatted_start_date,
                Submission.reporting_end_date == formatted_end_date,
                Submission.is_quarter_format == request.json.get('is_quarter'),
                Submission.d2_submission.is_(False),
                Submission.publish_status_id != PUBLISH_STATUS_DICT['unpublished'])

            if 'existing_submission_id' in request.json:
                submissions.filter(Submission.submission_id !=
                                   request.json['existing_submission_id'])

            submissions = submissions.order_by(desc(Submission.created_at))

            if submissions.count() > 0:
                data = {
                        "message": "A submission with the same period already exists.",
                        "submissionId": submissions[0].submission_id
                }
                return JsonResponse.create(StatusCode.CLIENT_ERROR, data)

        return file_manager.submit(create_credentials)

    @app.route("/v1/finalize_job/", methods=["POST"])
    @requires_login
    @use_kwargs({'upload_id': webargs_fields.Int(required=True)})
    def finalize_submission(upload_id):
        file_manager = FileHandler(request, is_local=is_local, server_path=server_path)
        return file_manager.finalize(upload_id)

    @app.route("/v1/check_status/", methods=["POST"])
    @convert_to_submission_id
    @requires_submission_perms('reader')
    def check_status(submission):
        return get_status(submission)

    @app.route("/v1/window/", methods=["GET"])
    def window():
        current_windows = get_window()

        data = []

        if current_windows.count() is 0:
            data = None
        else:
            for window in current_windows:
                data.append({
                             'start_date': str(window.start_date),
                             'end_date': str(window.end_date),
                             'notice_block': window.block_certification,
                             'message': window.message,
                             'type': window.application_type.application_name
                })

        return JsonResponse.create(StatusCode.OK, {"data": data})

    @app.route("/v1/submission_error_reports/", methods=["POST"])
    @requires_login
    @use_kwargs({'submission_id': webargs_fields.Int(required=True)})
    def submission_error_reports(submission_id):
        file_manager = FileHandler(request, is_local=is_local, server_path=server_path)
        return file_manager.get_error_report_urls_for_submission(submission_id)

    @app.route("/v1/submission_warning_reports/", methods=["POST"])
    @requires_login
    @use_kwargs({'submission_id': webargs_fields.Int(required=True)})
    def submission_warning_reports(submission_id):
        file_manager = FileHandler(request, is_local=is_local, server_path=server_path)
        return file_manager.get_error_report_urls_for_submission(submission_id, is_warning=True)

    @app.route("/v1/error_metrics/", methods=["POST"])
    @convert_to_submission_id
    @requires_submission_perms('reader')
    def submission_error_metrics(submission):
        return get_error_metrics(submission)

    @app.route("/v1/local_upload/", methods=["POST"])
    @requires_login
    def upload_local_file():
        file_manager = FileHandler(request, is_local=is_local, server_path=server_path)
        return file_manager.upload_file()

    @app.route("/v1/list_submissions/", methods=["GET"])
    @requires_login
    @use_kwargs({
        'page': webargs_fields.Int(missing=1),
        'limit': webargs_fields.Int(missing=5),
        'certified': webargs_fields.String(
            required=True,
            validate=webargs_validate.OneOf(('mixed', 'true', 'false'))),
        'sort': webargs_fields.String(missing='modified'),
        'order': webargs_fields.String(missing='desc'),
        'd2_submission': webargs_fields.Bool(missing=False),
    })
    def list_submissions(page, limit, certified, sort, order, d2_submission):
        """ List submission IDs associated with the current user """
        return list_submissions_handler(page, limit, certified, sort, order, d2_submission)

    @app.route("/v1/list_certifications/", methods=["POST"])
    @convert_to_submission_id
    @requires_submission_perms('reader')
    def submission_list_certifications(submission):
        if submission.d2_submission:
            return JsonResponse.error(ValueError("FABS submissions do not have a certification history"),
                                      StatusCode.CLIENT_ERROR)

        sess = GlobalDB.db().session

        certify_history = sess.query(CertifyHistory).filter_by(submission_id=submission.submission_id)

        if certify_history.count() == 0:
            return JsonResponse.error(ValueError("This submission has no certification history"),
                                      StatusCode.CLIENT_ERROR)

        return list_certifications(submission)

    @app.route("/v1/get_certified_file/", methods=["POST"])
    @use_kwargs({
        'submission_id': webargs_fields.Int(required=True),
        'certified_files_history_id': webargs_fields.Int(required=True),
        'is_warning': webargs_fields.Bool(missing=False)
    })
    @requires_submission_perms('reader')
    def get_certified_file(submission, certified_files_history_id, is_warning):
        """ Get the signed URL for the specified file history """
        return file_history_url(submission, certified_files_history_id, is_warning, is_local)

    @app.route("/v1/get_protected_files/", methods=["GET"])
    @requires_login
    def get_protected_files():
        """ Return signed URLs for all help page files """
        file_manager = FileHandler(request, is_local=is_local, server_path=server_path)
        return file_manager.get_protected_files()

    @app.route("/v1/check_current_page/", methods=["GET"])
    @convert_to_submission_id
    @requires_submission_perms('reader')
    def check_current_page(submission):

        sess = GlobalDB.db().session

        submission_id = submission.submission_id

        # /v1/uploadDetachedFiles/
        # DetachedFiles
        if submission.d2_submission:
            data = {
                "message": "The current progress of this submission ID is on /v1/uploadDetachedFiles/ page.",
                "step": "6"
            }
            return JsonResponse.create(StatusCode.OK, data)

        # /v1/reviewData/
        # Checks that both E and F files are finished
        review_data = sess.query(Job).filter(Job.submission_id == submission_id,
                                             Job.file_type_id.in_([6, 7]), Job.job_status_id == 4)

        # Need to check that cross file is done as well
        generate_ef = sess.query(Job).filter(Job.submission_id == submission_id, Job.job_type_id == 4,
                                             Job.number_of_errors == 0, Job.job_status_id == 4)

        if review_data.count() == 2 and generate_ef.count() > 0:
            data = {
                "message": "The current progress of this submission ID is on /v1/reviewData/ page.",
                "step": "5"
            }
            return JsonResponse.create(StatusCode.OK, data)

        # /v1/generateEF/
        if generate_ef.count() > 0:
            data = {
                "message": "The current progress of this submission ID is on /v1/generateEF/ page.",
                "step": "4"
            }
            return JsonResponse.create(StatusCode.OK, data)

        validate_cross_file = sess.query(Job).filter(Job.submission_id == submission_id,
                                                     Job.file_type_id.in_([4, 5]), Job.job_type_id == 2,
                                                     Job.number_of_errors == 0, Job.file_size.isnot(None),
                                                     Job.job_status_id == 4)

        generate_files = sess.query(Job).filter(Job.submission_id == submission_id,
                                                Job.file_type_id.in_([1, 2, 3]), Job.job_type_id == 2,
                                                Job.number_of_errors == 0, Job.file_size.isnot(None),
                                                Job.job_status_id == 4)

        # /v1/validateCrossFile/
        if validate_cross_file.count() == 2 and generate_files.count() == 3:
            data = {
                "message": "The current progress of this submission ID is on /v1/validateCrossFile/ page.",
                "step": "3"
            }
            return JsonResponse.create(StatusCode.OK, data)

        # /v1/generateFiles/
        if generate_files.count() == 3:
            data = {
                "message": "The current progress of this submission ID is on /v1/generateFiles/ page.",
                "step": "2"
            }
            return JsonResponse.create(StatusCode.OK, data)

        # /v1/validateData/
        validate_data = sess.query(Job).filter(Job.submission_id == submission_id,
                                               Job.file_type_id.in_([1, 2, 3]), Job.job_type_id == 2,
                                               Job.number_of_errors != 0, Job.file_size.isnot(None))
        check_header_errors = sess.query(Job).filter(Job.submission_id == submission_id,
                                                     Job.file_type_id.in_([1, 2, 3]), Job.job_type_id == 2,
                                                     Job.job_status_id != 4, Job.file_size.isnot(None))
        if validate_data.count() or check_header_errors.count() > 0:
            data = {
                    "message": "The current progress of this submission ID is on /v1/validateData/ page.",
                    "step": "1"
            }
            return JsonResponse.create(StatusCode.OK, data)

        else:
            return JsonResponse.error(ValueError("The submisssion ID returns no response"), StatusCode.CLIENT_ERROR)

    @app.route("/v1/generate_file/", methods=["POST"])
    @convert_to_submission_id
    @use_kwargs({'file_type': webargs_fields.String(
        required=True,
        validate=webargs_validate.OneOf(FILE_TYPE_DICT_LETTER.values())
    )})
    def generate_file(submission_id, file_type):
        """ Generate file from external API """
        file_manager = FileHandler(request, is_local=is_local, server_path=server_path)
        return file_manager.generate_file(submission_id, file_type)

    @app.route("/v1/generate_detached_file/", methods=["POST"])
    @requires_login
    @use_kwargs({
        'file_type': webargs_fields.String(
            required=True, validate=webargs_validate.OneOf(('D1', 'D2'))),
        'cgac_code': webargs_fields.String(),
        'frec_code': webargs_fields.String(),
        'start': webargs_fields.String(required=True),
        'end': webargs_fields.String(required=True)
    })
    def generate_detached_file(file_type, cgac_code, frec_code, start, end):
        """ Generate a file from external API, independent from a submission """
        if not cgac_code and not frec_code:
            return JsonResponse.error(ValueError("Detached file generation requires CGAC or FR Entity Code"),
                                      StatusCode.CLIENT_ERROR)

        file_manager = FileHandler(request, is_local=is_local, server_path=server_path)
        return file_manager.generate_detached_file(file_type, cgac_code, frec_code, start, end)

    @app.route("/v1/check_detached_generation_status/", methods=["POST"])
    @requires_login
    @use_kwargs({'job_id': webargs_fields.Int(required=True)})
    def check_detached_generation_status(job_id):
        """ Return status of file generation job """
        file_manager = FileHandler(request, is_local=is_local, server_path=server_path)
        return file_manager.check_detached_generation(job_id)

    @app.route("/v1/check_generation_status/", methods=["POST"])
    @convert_to_submission_id
    @requires_submission_perms('reader')
    @use_kwargs({'file_type': webargs_fields.String(
        required=True,
        validate=webargs_validate.OneOf(FILE_TYPE_DICT_LETTER.values()))
    })
    def check_generation_status(submission, file_type):
        """ Return status of file generation job """
        file_manager = FileHandler(request, is_local=is_local, server_path=server_path)
        return file_manager.check_generation(submission, file_type)

    @app.route("/v1/get_fabs_meta/", methods=["POST"])
    @convert_to_submission_id
    @requires_submission_perms('reader')
    def get_fabs_metadata(submission):
        """ Return metadata of FABS submission """
        return JsonResponse.create(StatusCode.OK, get_fabs_meta(submission.submission_id))

    @app.route("/v1/complete_generation/<generation_id>/", methods=["POST"])
    def complete_generation(generation_id):
        file_manager = FileHandler(request, is_local=is_local, server_path=server_path)
        return file_manager.complete_generation(generation_id)

    @app.route("/v1/upload_detached_file/", methods=["POST"])
    def upload_detached_file():
        file_manager = FileHandler(request, is_local=is_local, server_path=server_path)
        return file_manager.upload_detached_file(create_credentials)

    @app.route("/v1/submit_detached_file/", methods=["POST"])
    @convert_to_submission_id
    @requires_submission_perms('writer')
    def submit_detached_file(submission):
        file_manager = FileHandler(request, is_local=is_local, server_path=server_path)
        return file_manager.submit_detached_file(submission)

    @app.route("/v1/get_obligations/", methods=["POST"])
    @convert_to_submission_id
    @requires_submission_perms('reader')
    def get_obligations(submission):
        return JsonResponse.create(StatusCode.OK, get_submission_stats(submission.submission_id))

    @app.route("/v1/submission/<int:submission_id>/narrative", methods=['GET'])
    @requires_submission_perms('reader')
    def get_submission_narratives(submission):
        return narratives_for_submission(submission)

    @app.route("/v1/submission/<int:submission_id>/narrative", methods=['POST'])
    @requires_submission_perms('writer')
    def post_submission_narratives(submission):
        json = request.json or {}
        # clean input
        json = {key.upper(): value.strip() for key, value in json.items()
                if isinstance(value, str) and value.strip()}
        return update_narratives(submission, json)

    @app.route("/v1/submission/<int:submission_id>/report_url", methods=['POST'])
    @requires_submission_perms('reader')
    @use_kwargs({
        'warning': webargs_fields.Bool(),
        'file_type': webargs_fields.String(
            required=True,
            validate=webargs_validate.OneOf(FILE_TYPE_DICT.keys())
        ),
        'cross_type': webargs_fields.String(validate=webargs_validate.OneOf(FILE_TYPE_DICT.keys()))
    })
    def post_submission_report_url(submission, warning, file_type, cross_type):
        return submission_report_url(submission, bool(warning), file_type, cross_type)

    @app.route("/v1/delete_submission/", methods=['POST'])
    @convert_to_submission_id
    @requires_submission_perms('writer')
    def delete_submission(submission):
        """ Deletes all data associated with the specified submission
        NOTE: THERE IS NO WAY TO UNDO THIS """
        return delete_all_submission_data(submission)

    @app.route("/v1/check_year_quarter/", methods=["GET"])
    @requires_login
    @use_kwargs({'cgac_code': webargs_fields.String(),
                 'frec_code': webargs_fields.String(),
                 'reporting_fiscal_year': webargs_fields.String(required=True),
                 'reporting_fiscal_period': webargs_fields.String(required=True)})
    def check_year_and_quarter(cgac_code, frec_code, reporting_fiscal_year, reporting_fiscal_period):
        """ Check if cgac (or frec) code, year, and quarter already has a published submission """
        if not cgac_code and not frec_code:
            return JsonResponse.error(ValueError("CGAC or FR Entity Code required"), StatusCode.CLIENT_ERROR)

        sess = GlobalDB.db().session
        return find_existing_submissions_in_period(sess, cgac_code, frec_code, reporting_fiscal_year,
                                                   reporting_fiscal_period)

    @app.route("/v1/certify_submission/", methods=['POST'])
    @convert_to_submission_id
    @requires_submission_perms('submitter', check_owner=False)
    def certify_submission(submission):
        if not submission.publishable:
            return JsonResponse.error(ValueError("Submission cannot be certified due to critical errors"),
                                      StatusCode.CLIENT_ERROR)

        if not submission.is_quarter_format:
            return JsonResponse.error(ValueError("Monthly submissions cannot be certified"), StatusCode.CLIENT_ERROR)

        if submission.publish_status_id == PUBLISH_STATUS_DICT['published']:
            return JsonResponse.error(ValueError("Submission has already been certified"), StatusCode.CLIENT_ERROR)

        windows = get_window()
        for window in windows:
            if window.block_certification:
                return JsonResponse.error(ValueError(window.message), StatusCode.CLIENT_ERROR)

        sess = GlobalDB.db().session

        response = find_existing_submissions_in_period(sess, submission.cgac_code, submission.frec_code,
                                                       submission.reporting_fiscal_year,
                                                       submission.reporting_fiscal_period, submission.submission_id)

        if response.status_code == StatusCode.OK:
            sess = GlobalDB.db().session

            # create the certify_history entry
            certify_history = CertifyHistory(created_at=datetime.utcnow(), user_id=g.user.user_id,
                                             submission_id=submission.submission_id)
            sess.add(certify_history)
            sess.commit()

            # get the certify_history entry including the PK
            certify_history = sess.query(CertifyHistory).filter_by(submission_id=submission.submission_id).\
                order_by(CertifyHistory.created_at.desc()).first()

            # move files (locally we don't move but we still need to populate the certified_files_history table)
            file_manager = FileHandler(request, is_local=is_local, server_path=server_path)
            file_manager.move_certified_files(submission, certify_history, is_local)

            # set submission contents
            submission.certifying_user_id = g.user.user_id
            submission.publish_status_id = PUBLISH_STATUS_DICT['published']
            sess.commit()

        return response

    @app.route("/v1/restart_validation/", methods=['POST'])
    @convert_to_submission_id
    @requires_submission_perms('writer')
    @use_kwargs({'d2_submission': webargs_fields.Bool(missing=False)})
    def restart_validation(submission, d2_submission):
        return FileHandler.restart_validation(submission, d2_submission)


def convert_to_submission_id(fn):
    """Decorator which reads the request, looking for a submission key to
    convert into a submission_id parameter. The provided function should have
    a submission_id parameter as its first argument."""
    @wraps(fn)
    @requires_login     # check login before checking submission_id
    def wrapped(*args, **kwargs):
        req_args = webargs_parser.parse({
            'submission': webargs_fields.Int(),
            'submission_id': webargs_fields.Int()
        })
        submission_id = req_args.get('submission', req_args.get('submission_id'))
        if submission_id is None:
            raise ResponseException(
                "submission_id is required", StatusCode.CLIENT_ERROR)
        return fn(submission_id, *args, **kwargs)
    return wrapped


def find_existing_submissions_in_period(sess, cgac_code, frec_code, reporting_fiscal_year,
                                        reporting_fiscal_period, submission_id=None):
    submission_query = sess.query(Submission).filter(
        (Submission.cgac_code == cgac_code) if cgac_code else (Submission.frec_code == frec_code),
        Submission.reporting_fiscal_year == reporting_fiscal_year,
        Submission.reporting_fiscal_period == reporting_fiscal_period,
        Submission.publish_status_id != PUBLISH_STATUS_DICT['unpublished'])

    if submission_id:
        submission_query = submission_query.filter(
            Submission.submission_id != submission_id)

    submission_query = submission_query.order_by(desc(Submission.created_at))

    if submission_query.count() > 0:
        data = {
            "message": "A submission with the same period already exists.",
            "submissionId": submission_query[0].submission_id
        }
        return JsonResponse.create(StatusCode.CLIENT_ERROR, data)
    return JsonResponse.create(StatusCode.OK, {"message": "Success"})


def get_window():
    sess = GlobalDB.db().session

    curr_date = datetime.now().date()

    return sess.query(SubmissionWindow).filter(
                                            SubmissionWindow.start_date <= curr_date,
                                            SubmissionWindow.end_date >= curr_date)
