class ItselfSubdirException(Exception):
    """Exception raised when is required to move a directory into a subdirectory of itself."""
    def __init__(self):
        self.message = 'Itself subdirectory: the operation you required is not allowed'
        
    def get_message(self):
        """Method for getting the 'message' of the exception.
        
        Parameters
        ----------
        self --> ItselfSubdirException class, self reference to the object instance
        
        Returns
        -------
        self.message --> str, the message of the exception
        """
        return self.message
    
    def set_message(self, message):
        """Method for setting the 'message' of the exception.
        
        Parameters
        ----------
        self --> ItselfSubdirException class, self reference to the object instance
        message --> str, the message of the exception
        
        Returns
        -------
        None
        """
        self.message = message


class RootDirectoryException(Exception):
    """Exception raised when is required to delete/rename the root directory, even if the requirer is root."""
    def __init__(self):
        self.message = 'Root Directory: the operation you required is not allowed'
        
    def get_message(self):
        """Method for getting the 'message' of the exception.
        
        Parameters
        ----------
        self --> RootDirectoryException class, self reference to the object instance
        
        Returns
        -------
        self.message --> str, the message of the exception
        """
        return self.message
    
    def set_message(self, message):
        """Method for setting the 'message' of the exception.
        
        Parameters
        ----------
        self --> RootDirectoryException class, self reference to the object instance
        message --> str, the message of the exception
        
        Returns
        -------
        None
        """
        self.message = message


class AccessDeniedException(Exception):
    """Exception raised when who required the operation has not enough privileges."""
    def __init__(self, resource):
        self.message = 'Access denied: the operation you required is not allowed on {}'.format(resource)
        
    def get_message(self):
        """Method for getting the 'message' of the exception.
        
        Parameters
        ----------
        self --> AccessDeniedException class, self reference to the object instance
        
        Returns
        -------
        self.message --> str, the message of the exception
        """
        return self.message
    
    def set_message(self, message):
        """Method for setting the 'message' of the exception.
        
        Parameters
        ----------
        self --> AccessDeniedException class, self reference to the object instance
        message --> str, the message of the exception
        
        Returns
        -------
        None
        """
        self.message = message
        
        
class NotFoundException(Exception):
    """Exception raised when a path is not found because it does not exist."""
    def __init__(self, path):
        self.message = 'The path does not exist: "{}" not found'.format(path)
        
    def get_message(self):
        """Method for getting the 'message' of the exception.
        
        Parameters
        ----------
        self --> NotFoundException class, self reference to the object instance
        
        Returns
        -------
        self.message --> str, the message of the exception
        """
        return self.message
    
    def set_message(self, message):
        """Method for setting the 'message' of the exception.
        
        Parameters
        ----------
        self --> NotFoundException class, self reference to the object instance
        message --> str, the message of the exception
        
        Returns
        -------
        None
        """
        self.message = message
        

class RootNecessaryException(Exception):
    """Exception raised when who required the operation has not root privileges."""
    def __init__(self):
        self.message = 'Operation not allowed: you MUST be root'
        
    def get_message(self):
        """Method for getting the 'message' of the exception.
        
        Parameters
        ----------
        self --> RootNecessaryException class, self reference to the object instance
        
        Returns
        -------
        self.message --> str, the message of the exception
        """
        return self.message
    
    def set_message(self, message):
        """Method for setting the 'message' of the exception.
        
        Parameters
        ----------
        self --> RootNecessaryException class, self reference to the object instance
        message --> str, the message of the exception
        
        Returns
        -------
        None
        """
        self.message = message
        

class NotDirectoryException(Exception):
    """Exception raised when an operation allowed for a file is required 
       on a resource which is not a file."""
    def __init__(self, fake_dir):
        self.message = 'Cannot create the directory: "{}" is not a directory'.format(fake_dir)
        
    def get_message(self):
        """Method for getting the 'message' of the exception.
        
        Parameters
        ----------
        self --> NotDirectoryException class, self reference to the object instance
        
        Returns
        -------
        self.message --> str, the message of the exception
        """
        return self.message
    
    def set_message(self, message):
        """Method for setting the 'message' of the exception.
        
        Parameters
        ----------
        self --> NotDirectoryException class, self reference to the object instance
        message --> str, the message of the exception
        
        Returns
        -------
        None
        """
        self.message = message
        

class NotParentException(Exception):
    """Exception raised when a parent/ancestor directory does not exist."""
    def __init__(self, ne_dir):
        self.message = 'Parent directory "{}" does not exists'.format(ne_dir)
        
    def get_message(self):
        """Method for getting the 'message' of the exception.
        
        Parameters
        ----------
        self --> NotParentException class, self reference to the object instance
        
        Returns
        -------
        self.message --> str, the message of the exception
        """
        return self.message
    
    def set_message(self, message):
        """Method for setting the 'message' of the exception.
        
        Parameters
        ----------
        self --> NotParentException class, self reference to the object instance
        message --> str, the message of the exception
        
        Returns
        -------
        None
        """
        self.message = message
        
    
class AlreadyExistsException(Exception):
    """Exception raised when a it's required to create a resource which already exists."""
    def __init__(self):
        self.message = 'The resource already exists'
        
    def get_message(self):
        """Method for getting the 'message' of the exception.
        
        Parameters
        ----------
        self --> AlreadyExistsException class, self reference to the object instance
        
        Returns
        -------
        self.message --> str, the message of the exception
        """
        return self.message
    
    def set_message(self, message):
        """Method for setting the 'message' of the exception.
        
        Parameters
        ----------
        self --> AlreadyExistsException class, self reference to the object instance
        message --> str, the message of the exception
        
        Returns
        -------
        None
        """
        self.message = message
        

class AlreadyExistsDirectoryException(Exception):
    """Exception raised when a it's required to put a file whose name is already a direcotry."""
    def __init__(self):
        self.message = 'A directory with the same name already exists'
        
    def get_message(self):
        """Method for getting the 'message' of the exception.
        
        Parameters
        ----------
        self --> AlreadyExistsDirectoryException class, self reference to the object instance
        
        Returns
        -------
        self.message --> str, the message of the exception
        """
        return self.message
    
    def set_message(self, message):
        """Method for setting the 'message' of the exception.
        
        Parameters
        ----------
        self --> AlreadyExistsDirectoryException class, self reference to the object instance
        message --> str, the message of the exception
        
        Returns
        -------
        None
        """
        self.message = message
        
        
class NotEmptyException(Exception):
    """Exception raised when it's required to delete a directory which is not empty."""
    def __init__(self):
        self.message = 'The directory is not empty'
        
    def get_message(self):
        """Method for getting the 'message' of the exception.
        
        Parameters
        ----------
        self --> NotEmptyException class, self reference to the object instance
        
        Returns
        -------
        self.message --> str, the message of the exception
        """
        return self.message
    
    def set_message(self, message):
        """Method for setting the 'message' of the exception.
        
        Parameters
        ----------
        self --> NotEmptyException class, self reference to the object instance
        message --> str, the message of the exception
        
        Returns
        -------
        None
        """
        self.message = message
        
        
class AccessDeniedAtLeastOneException(Exception):
    """Exception raised when who required the operation has not 
       enough privileges at least on one resource (for recoursive operations)."""
    def __init__(self, resource):
        self.message = 'Access denied at least on one resource: "{}"'.format(resource)
        
    def get_message(self):
        """Method for getting the 'message' of the exception.
        
        Parameters
        ----------
        self --> AccessDeniedAtLeastOneException class, self reference to the object instance
        
        Returns
        -------
        self.message --> str, the message of the exception
        """
        return self.message
    
    def set_message(self, message):
        """Method for setting the 'message' of the exception.
        
        Parameters
        ----------
        self --> AccessDeniedAtLeastOneException class, self reference to the object instance
        message --> str, the message of the exception
        
        Returns
        -------
        None
        """
        self.message = message
        
        
class InvalidModException(Exception):
    """Exception raised when in chmod it is passed a mod in a bad format."""
    def __init__(self,mode):
        self.message = 'invalid mode {}'.format(mode)
        
    def get_message(self):
        """Method for getting the 'message' of the exception.
        
        Parameters
        ----------
        self --> InvalidModException class, self reference to the object instance
        
        Returns
        -------
        self.message --> str, the message of the exception
        """
        return self.message
    
    def set_message(self, message):
        """Method for setting the 'message' of the exception.
        
        Parameters
        ----------
        self --> InvalidModException class, self reference to the object instance
        message --> str, the message of the exception
        
        Returns
        -------
        None
        """
        self.message = message
        
        
class GroupAlreadyExistsException(Exception):
    """Exception raised when it's required to create a group which already exists."""
    def __init__(self):
        self.message = 'The group already exists'
        
    def get_message(self):
        """Method for getting the 'message' of the exception.
        
        Parameters
        ----------
        self --> GroupAlreadyExistsException class, self reference to the object instance
        
        Returns
        -------
        self.message --> str, the message of the exception
        """
        return self.message
    
    def set_message(self, message):
        """Method for setting the 'message' of the exception.
        
        Parameters
        ----------
        self --> GroupAlreadyExistsException class, self reference to the object instance
        message --> str, the message of the exception
        
        Returns
        -------
        None
        """
        self.message = message
        
        
class UserAlreadyExistsException(Exception):
    """Exception raised when it's required to create a user which already exists."""
    def __init__(self):
        self.message = 'The user already exists'
        
    def get_message(self):
        """Method for getting the 'message' of the exception.
        
        Parameters
        ----------
        self --> UserAlreadyExistsException class, self reference to the object instance
        
        Returns
        -------
        self.message --> str, the message of the exception
        """
        return self.message
    
    def set_message(self, message):
        """Method for setting the 'message' of the exception.
        
        Parameters
        ----------
        self --> UserAlreadyExistsException class, self reference to the object instance
        message --> str, the message of the exception
        
        Returns
        -------
        None
        """
        self.message = message
        
        
class UserNotFoundException(Exception):
    """Exception raised when it's required an operation on a user which does not exist."""
    def __init__(self, usr):
        self.message = 'The user does not exist: {}'.format(usr)
        
    def get_message(self):
        """Method for getting the 'message' of the exception.
        
        Parameters
        ----------
        self --> UserNotFoundException class, self reference to the object instance
        
        Returns
        -------
        self.message --> str, the message of the exception
        """
        return self.message
    
    def set_message(self, message):
        """Method for setting the 'message' of the exception.
        
        Parameters
        ----------
        self --> UserNotFoundException class, self reference to the object instance
        message --> str, the message of the exception
        
        Returns
        -------
        None
        """
        self.message = message
        
        
class GroupNotFoundException(Exception):
    """Exception raised when it's required an operation on a group which does not exist."""
    def __init__(self, grp):
        self.message = 'The group does not exist: {}'.format(grp)
        
    def get_message(self):
        """Method for getting the 'message' of the exception.
        
        Parameters
        ----------
        self --> GroupNotFoundException class, self reference to the object instance
        
        Returns
        -------
        self.message --> str, the message of the exception
        """
        return self.message
    
    def set_message(self, message):
        """Method for setting the 'message' of the exception.
        
        Parameters
        ----------
        self --> GroupNotFoundException class, self reference to the object instance
        message --> str, the message of the exception
        
        Returns
        -------
        None
        """
        self.message = message
        

class MainUserGroupException(Exception):
    """Exception raised when it's required to delete a group which is main group of a user."""
    def __init__(self, grp):
        self.message = 'The group {} is the main group of a user: please remove the user first'.format(grp)
        
    def get_message(self):
        """Method for getting the 'message' of the exception.
        
        Parameters
        ----------
        self --> MainUserGroupException class, self reference to the object instance
        
        Returns
        -------
        self.message --> str, the message of the exception
        """
        return self.message
    
    def set_message(self, message):
        """Method for setting the 'message' of the exception.
        
        Parameters
        ----------
        self --> MainUserGroupException class, self reference to the object instance
        message --> str, the message of the exception
        
        Returns
        -------
        None
        """
        self.message = message
        
        
class InvalidSyntaxException(Exception):
    """Exception raised when the syntax of a command is wrong."""
    def __init__(self, syntax):
        self.message = 'Invalid syntax, the syntax should be like: {}'.format(syntax)
        
    def get_message(self):
        """Method for getting the 'message' of the exception.
        
        Parameters
        ----------
        self --> InvalidSyntaxException class, self reference to the object instance
        
        Returns
        -------
        self.message --> str, the message of the exception
        """
        return self.message
    
    def set_message(self, message):
        """Method for setting the 'message' of the exception.
        
        Parameters
        ----------
        self --> InvalidSyntaxException class, self reference to the object instance
        message --> str, the message of the exception
        
        Returns
        -------
        None
        """
        self.message = message
        
        
class CommandNotFoundException(Exception):
    """Exception raised when a command does not exist."""
    def __init__(self):
        self.message = 'Command not found'
        
    def get_message(self):
        """Method for getting the 'message' of the exception.
        
        Parameters
        ----------
        self --> CommandNotFoundException class, self reference to the object instance
        
        Returns
        -------
        self.message --> str, the message of the exception
        """
        return self.message
    
    def set_message(self, message):
        """Method for setting the 'message' of the exception.
        
        Parameters
        ----------
        self --> CommandNotFoundException class, self reference to the object instance
        message --> str, the message of the exception
        
        Returns
        -------
        None
        """
        self.message = message
        

class GetFileException(Exception):
    """Exception raised when get_file doesn't end properly."""
    def __init__(self):
        self.message = 'Unable to get the file'
        
    def get_message(self):
        """Method for getting the 'message' of the exception.
        
        Parameters
        ----------
        self --> GetFileException class, self reference to the object instance
        
        Returns
        -------
        self.message --> str, the message of the exception
        """
        return self.message
    
    def set_message(self, message):
        """Method for setting the 'message' of the exception.
        
        Parameters
        ----------
        self --> GetFileException class, self reference to the object instance
        message --> str, the message of the exception
        
        Returns
        -------
        None
        """
        self.message = message
        
        