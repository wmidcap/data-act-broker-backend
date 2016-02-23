import uuid
from sqlalchemy.orm.exc import MultipleResultsFound
from flask.ext.bcrypt import Bcrypt, generate_password_hash, check_password_hash
from dataactcore.models.userModel import User, UserStatus, EmailToken, EmailTemplateType , EmailTemplate, PermissionType
from dataactcore.models.userInterface import UserInterface
from dataactcore.utils.responseException import ResponseException
from dataactcore.utils.statusCode import StatusCode

class UserHandler(UserInterface):
    """ Responsible for all interaction with the user database

    Class Fields:
    dbName -- Name of user database
    credentialsFile -- This file should store a JSON with keys "username" and "password" for the database

    Instance Fields:
    engine -- sqlalchemy engine for creating connections and sessions to database
    connection -- sqlalchemy connection to user database
    session - sqlalchemy session for ORM calls to user database
    """
    HASH_ROUNDS = 12 # How many rounds to use for hashing passwords

    def getTokenSalt(self,token):
        """gets the salt from a given token so it can be decoded"""
        return  self.session.query(EmailToken.salt).filter(EmailToken.token == token).one()

    def saveToken(self,salt,token):
        newToken = EmailToken()
        newToken.salt = salt
        newToken.token = token
        self.session.add(newToken)
        self.session.commit()

    def deleteToken(self,token):
        oldToken = self.session.query(EmailToken).filter(EmailToken.token == token).one()
        self.session.delete(oldToken)
        self.session.commit()


    def getUserByUID(self,uid):
        """ Return a User object that matches specified email """
        result = self.session.query(User).filter(User.user_id == uid).all()
        # Raise exception if we did not find exactly one user
        self.checkUnique(result,"No users with that email", "Multiple users with that email")
        return result[0]

    def getUserId(self, username):
        """ Find an id for specified username, creates a new entry for new usernames, raises an exception if multiple results found

        Arguments:
        username - username to find an id for
        Returns:
        user_id to be used by session handler
        """
        # Check if user exists
        queryResult = self.session.query(User.user_id).filter(User.username == username).all()
        if(len(queryResult) == 1):
            # If so, return ID
            return queryResult[0].user_id
        elif(len(queryResult) == 0):
            # If not, add new user
            newUser = User(username = username)
            self.session.add(newUser)
            self.session.commit()
            return newUser.user_id
        else:
            # Multiple entries for this user, server error
            raise MultipleResultsFound("Multiple entries for single username")

    def getUserByEmail(self,email):
        """ Return a User object that matches specified email """
        result = self.session.query(User).filter(User.email == email).all()
        # Raise exception if we did not find exactly one user
        self.checkUnique(result,"No users with that email", "Multiple users with that email")
        return result[0]


    def addUserInfo(self,user,name,agency,title):
        """ Called after registration, add all info to user. """
        # Add info to user ORM
        user.name = name
        user.agency = agency
        user.title = title
        self.session.commit()

    def changeStatus(self,user,statusName):
        """ Change status for specified user """
        try:
            user.user_status_id = UserStatus.getStatus(statusName)
        except ValueError as e:
            # In this case having a bad status name is a client error
            raise ResponseException(str(e),StatusCode.CLIENT_ERROR,ValueError)
        self.session.commit()

    def addUnconfirmedEmail(self,email):
        """ Create user with specified email """
        user = User(email = email)
        self.changeStatus(user,"awaiting_confirmation")
        self.setPermission(user,0) # Users start with no permissions
        self.session.add(user)
        self.session.commit()

    def getEmailTemplate(self,emailType):
        emailId = self.session.query(EmailTemplateType.email_template_type_id).filter(EmailTemplateType.name == emailType).one()
        return self.session.query(EmailTemplate).filter(EmailTemplate.template_type_id == emailId).one()

    def getUsersByStatus(self,status):
        """ Return list of all users with specified status """
        statusId = UserStatus.getStatus(status)
        return self.session.query(User).filter(User.user_status_id == statusId).all()

    def getStatusOfUser(self,user):
        """ Given a user object return their status as a string """
        return user.status.name

    def getStatusIdOfUser(self,user):
        """ Given a user object return status ID """
        return user.status_id

    def getUsersByType(self,permissionName):
        """ Get all users that have the specified permission """
        userList = []
        bitNumber = self.getPermissionId(permissionName)
        users = self.session.query(User).all()
        for user in users:
            if self.checkPermission(user,bitNumber):
                # This user has this permission, include them in list
                userList.append(user)
        return userList


    def hasPermisson(self,user,permissionName):
        """ Checks if user specified permission """
        bitNumber = self.getPermissionId(permissionName)
        if self.checkPermission(user,bitNumber):
            return True
        return False


    @staticmethod
    def checkPermission(user,bitNumber):
        """ Check whether user has the specified permission, determined by whether a binary representation of user's permissions has the specified bit set to 1 """
        if(user.permissions == None):
            # This user has no permissions
            return False
        bitValue = 2 ** (bitNumber)
        lowEnd = user.permissions % (bitValue * 2) # This leaves the bit we care about as the highest remaining
        return (lowEnd >= bitValue) # If the number is still at least the bitValue, we have that permission

    def setPermission(self,user,permission):
        """ Define a user's permission to set value """
        user.permissions = permission
        self.session.commit()

    def grantPermission(self,user,permissionName):
        """ Grant a user a permission specified by name """
        if(user.permissions == None):
            # Start users with zero permissions
            user.permissions = 0
        bitNumber = self.getPermissionId(permissionName)
        if not self.checkPermission(user,bitNumber):
            # User does not have permission, grant it
            user.permissions = user.permissions + (2 ** bitNumber)
            self.session.commit()

    def removePermission(self,user,permissionName):
        """ Grant a user a permission specified by name """
        if(user.permissions == None):
            # Start users with zero permissions
            user.permissions = 0
        bitNumber = self.getPermissionId(permissionName)
        if self.checkPermission(user,bitNumber):
            # User has permission, remove it
            user.permissions = user.permissions - (2 ** bitNumber)
            self.session.commit()

    def getPermissionId(self,permissionName):
        result = self.session.query(PermissionType).filter(PermissionType.name == permissionName).all()
        self.checkUnique(result,"Not a valid user type","Multiple permission entries for that type")
        return result[0].permission_type_id

    def checkPassword(self,user,password,bcrypt):
        """ Given a user object and a password, verify that the password is correct.  Returns True if valid password, False otherwise. """
        # Check the password with bcrypt
        return bcrypt.check_password_hash(user.password_hash,password+user.salt)

    def setPassword(self,user,password,bcrypt):
        """ Given a user and a new password, changes the hashed value in the database to match new password.  Returns True if successful. """
        # Generate hash with bcrypt and store it
        newSalt =  uuid.uuid4().hex
        user.salt = newSalt
        user.password_hash = bcrypt.generate_password_hash(password+newSalt,UserHandler.HASH_ROUNDS)
        self.session.commit()
        return True

    def clearPassword(self,user):
        """ Clear a user's password as part of reset process """
        user.salt = None
        user.password_hash = None
        self.session.commit()

    def createUser(self,email,password,bcrypt,admin=False):
        """creates a valid user"""
        user = User(email = email)
        self.session.add(user)
        self.setPassword(user,password,bcrypt)
        self.changeStatus(user,"approved")
        if(admin):
            self.setPermission(user,2)
        else:
            self.setPermission(user,1)
        self.session.commit()
