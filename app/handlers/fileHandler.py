import json
import sys
import traceback
from aws.s3UrlHandler import s3UrlHandler
from utils.requestDictionary import RequestDictionary
from jobHandler import JobHandler
from utils.jsonResponse import JsonResponse

class FileHandler:
    """ Responsible for all tasks relating to file upload

    Static fields:
    FILE_TYPES -- list of file labels that can be included

    Instance fields:
    request -- A flask request object, comes with the request
    response -- A flask response object, created with flask.Response()
    s3manager -- instance of s3UrlHandler, manages calls to S3
    """


    FILE_TYPES = ["appropriations","award_financial","award","procurement"]

    def __init__(self,request,response):
        """

        Arguments:
        request -- A flask request object, comes with the request
        response -- A flask response object, created with flask.Response()
        """
        self.request = request
        self.response = response

    # Submit set of files
    def submit(self,name):
        """ Builds S3 URLs for a set of files and adds all related jobs to job tracker database

        Flask request should include keys from FILE_TYPES class variable above

        Arguments:
        name -- User ID from the session handler

        Returns:
        Flask response returned will have key_url and key_id for each key in the request
        key_url is the S3 URL for uploading
        key_id is the job id to be passed to the finalize_submission route
        """
        self.s3manager = s3UrlHandler("reviewfile",name)
        responseDict= {}
        self.response.headers["Content-Type"] = "application/json"
        try:
            jobManager = JobHandler()
            fileNameMap = []

            safeDictionary = RequestDictionary(self.request)
            for fileName in FileHandler.FILE_TYPES :
                if( safeDictionary.exists(fileName)) :
                    responseDict[fileName+"_url"] = self.s3manager.getSignedUrl(safeDictionary.getValue(fileName))
                    fileNameMap.append((fileName,self.s3manager.s3FileName))

            fileJobDict = jobManager.createJobs(fileNameMap)
            for fileName in fileJobDict.keys():
                responseDict[fileName+"_id"] = fileJobDict[fileName]
            return JsonResponse.create(200,responseDict)
        except (ValueError , TypeError, NotImplementedError) as e:
            return JsonResponse.error(e,400)
        except Exception as e:
            # Unexpected exception, this is a 500 server error
            return JsonResponse.error(e,500)

    def finalize(self):
        """ Set upload job in job tracker database to finished, allowing dependent jobs to be started

        Flask request should include key "upload_id", which holds the job_id for the file_upload job

        Returns:
        A flask response object, if successful just contains key "success" with value True, otherwise value is False
        """
        self.response.headers["Content-Type"] = "application/json"
        responseDict = {}
        try:
            inputDictionary = RequestDictionary(self.request)
            jobId = inputDictionary.getValue("upload_id")
            # Change job status to finished
            jobManager = JobHandler()
            jobManager.changeToFinished(jobId)
            self.response.status_code = 200
            responseDict["success"] = True
            self.response.set_data(json.dumps(responseDict))
            return self.response
        except ( ValueError , TypeError ) as e:
            return JsonResponse.error(e,400)
        except Exception as e:
            # Unexpected exception, this is a 500 server error
            return JsonResponse.error(e,500)
