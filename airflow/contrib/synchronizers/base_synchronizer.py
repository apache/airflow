class BaseSynchronizer(object):
    def __init__(self):
        """
        Class to derive from in order update the dag folder by for example
        git, pull from another folder
        """

    def sync(self):
        """
        Sync will be called when an update is requested
        """
        pass


# for testing purposes
def get_instance():
    return BaseSynchronizer()

