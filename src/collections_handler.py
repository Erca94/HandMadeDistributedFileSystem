def get_fs(client):
    """Return the db containing file system metadata.
    
    Parameters
    ----------
    client --> pymongo.mongo_client.MongoClient class, MongoDB client
    
    Returns
    -------
    metadatafs['fs'] --> pymongo.collection.Collection, reference to collection fs
    """
    #get the MongoDb collection called "fs"
    metadatafs = client['metadatafs']
    return metadatafs['fs']


def get_users(client):
    """Return the db containing users metadata.
    
    Parameters
    ----------
    client --> pymongo.mongo_client.MongoClient class, MongoDB client
    
    Returns
    -------
    metadatafs['fs'] --> pymongo.collection.Collection, reference to collection users
    """
    #get the MongoDb collection called "users"
    metadatafs = client['metadatafs']
    return metadatafs['users']


def get_groups(client):
    """Return the db containing groups metadata.
    
    Parameters
    ----------
    client --> pymongo.mongo_client.MongoClient class, MongoDB client
    
    Returns
    -------
    metadatafs['fs'] --> pymongo.collection.Collection, reference to collection groups
    """
    #get the MongoDb collection called "groups"
    metadatafs = client['metadatafs']
    return metadatafs['groups']


def get_trash(client):
    """Return the db containing trash chunks to delete.
    
    Parameters
    ----------
    client --> pymongo.mongo_client.MongoClient class, MongoDB client
    
    Returns
    -------
    metadatafs['fs'] --> pymongo.collection.Collection, reference to collection trash
    """
    #get the MongoDb collection called "trash"
    metadatafs = client['metadatafs']
    return metadatafs['trash']

