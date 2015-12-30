import sys
import flask
from threading import Thread
from flask import Flask, request, make_response, session, g, redirect, url_for, \
     abort, render_template, flash ,session, Response, copy_current_request_context
import json
#open("pathLog","w").write(str(sys.path))
from handlers.validationManager import ValidationManager
from dataactcore.utils.jsonResponse import JsonResponse
from dataactcore.utils.statusCode import StatusCode
from dataactcore.utils.responseException import ResponseException
from csv import Error
from interfaces.jobTrackerInterface import JobTrackerInterface
from interfaces.errorInterface import ErrorInterface
from handlers.validationError import ValidationError

debugFlag = True

# Create application
app = Flask(__name__)
app.config.from_object(__name__)

validationManager = ValidationManager()

@app.route("/",methods=["GET"])
def testApp():
    return "Validator is running"

@app.route("/validate_threaded/",methods=["POST"])
def validate_threaded():

    def markJob(job,jobTracker,status) :
        try :
            jobTracker.markStatus(jobId,status)
        except Exception as e:
           pass

    @copy_current_request_context
    def ThreadedFunction (arg) :
            threadedManager = ValidationManager()
            threadedManager.threadedValidateJob(arg)

    jobId = None
    manager = ValidationManager()

    try :
        jobTracker = JobTrackerInterface()
    except ResponseException as e:
        exc = ResponseException(e.message)
        exc.wrappedException = e
        exc.status = StatusCode.CLIENT_ERROR
        return JsonResponse.error(exc,exc.status,{"table":"cannot connect to job database"})
    except Exception as e:
        exc = ResponseException(e.message)
        exc.wrappedException = e
        exc.status = StatusCode.INTERNAL_ERROR
        return JsonResponse.error(exc,exc.status,{"table":"cannot connect to job database"})

    try:
        jobId = manager.getJobID(flask.request)
    except ResponseException as e:
        exc = ResponseException(e.message)
        exc.wrappedException = e
        exc.status = StatusCode.CLIENT_ERROR
        manager.markJob(jobId,jobTracker,"invalid")
        errorHandler = ErrorInterface()
        errorHandler.writeFileError(jobId,manager.filename,ValidationError.unknownError)
        return JsonResponse.error(exc,exc.status,{"table":""})
    except Exception as e:
        exc = ResponseException(e.message)
        exc.wrappedException = e
        exc.status = StatusCode.CLIENT_ERROR
        manager.markJob(jobId,jobTracker,"invalid")
        errorHandler = ErrorInterface()
        errorHandler.writeFileError(jobId,manager.filename,ValidationError.unknownError)
        return JsonResponse.error(exc,exc.status,{"table":""})

    try:
        manager.testJobID(jobId)
    except ResponseException as e:
        exc = ResponseException(e.message)
        exc.wrappedException = e
        exc.status = StatusCode.CLIENT_ERROR
        errorHandler = ErrorInterface()
        errorHandler.writeFileError(jobId,manager.filename,ValidationError.jobError)
        return JsonResponse.error(exc,exc.status,{"table":""})
    except Exception as e:
        exc = ResponseException(e.message)
        exc.wrappedException = e
        exc.status = StatusCode.CLIENT_ERROR
        errorHandler = ErrorInterface()
        errorHandler.writeFileError(jobId,manager.filename,ValidationError.jobError)
        return JsonResponse.error(exc,exc.status,{"table":""})



    thread = Thread(target=ThreadedFunction, args= (jobId,))

    try :
        jobTracker.markStatus(jobId,"running")
    except Exception as e:
        exc = ResponseException(e.message)
        exc.wrappedException = e
        exc.status = StatusCode.INTERNAL_ERROR
        return JsonResponse.error(exc,exc.status,{"table":"could not start job"})

    thread.start()

    return JsonResponse.create(StatusCode.OK,{"table":"job"+str(jobId)})

@app.route("/validate/",methods=["POST"])
def validate():
    return validationManager.validateJob(request)

if __name__ == '__main__':
    app.run(debug=debugFlag,threaded=True,host="0.0.0.0")
