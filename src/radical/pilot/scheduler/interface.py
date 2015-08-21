#pylint: disable=C0301, C0103, W0212
"""
.. module:: radical.pilot.scheduler.Interface
   :platform: Unix
   :synopsis: The abstract interface class for all schedulers.

.. moduleauthor:: Ole Weidner <ole.weidner@rutgers.edu>
"""

__copyright__ = "Copyright 2013-2014, http://radical.rutgers.edu"
__license__   = "MIT"

from ..utils import logger


# -----------------------------------------------------------------------------
# 
class Scheduler(object):
    """Scheduler provides an abstsract interface for all schedulers.
    """

    # -------------------------------------------------------------------------
    # 
    def __init__(self, manager, session):
        """Le constructeur.
        """
        raise RuntimeError ('Not Implemented!')

    # -------------------------------------------------------------------------
    # 
    def add_pilots(self, pilots):
        """Inform the scheduler about new pilots"""

        logger.warn ("scheduler %s does not implement 'add_pilots()'" % self.name)

    # -------------------------------------------------------------------------
    # 
    def remove_pilots (self, pids):
        """Inform the scheduler about pilot removal"""

        logger.warn ("scheduler %s does not implement 'remove_pilots()'" % self.name)

    # -------------------------------------------------------------------------
    # 
    def get_pilots(self):
        """get a list of pilot instances as used by the scheduler"""

        logger.warn ("scheduler %s does not implement 'get_pilots()'" % self.name)

    # -------------------------------------------------------------------------
    # 
    def schedule (self, units) :
        """Schedules one or more ComputeUnits"""

        raise RuntimeError ("scheduler %s does not implement 'schedule()'" % self.name)

    # -------------------------------------------------------------------------
    # 
    def unschedule (self, units) :
        """Unschedule one or more ComputeUnits"""

        logger.warn ("scheduler %s does not implement 'unschedule()'" % self.name)

    # -------------------------------------------------------------------------
    # 
    @property
    def name(self):
        """The name of the scheduler"""
        return self.__class__.__name__

# -----------------------------------------------------------------------------

