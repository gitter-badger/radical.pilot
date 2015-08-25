    #pylint: disable=C0301, C0103, W0212

"""
.. module:: radical.pilot.unit_manager
   :platform: Unix
   :synopsis: Implementation of the UnitManager class.

.. moduleauthor:: Ole Weidner <ole.weidner@rutgers.edu>
"""

__copyright__ = "Copyright 2013-2014, http://radical.rutgers.edu"
__license__ = "MIT"

import os
import time
import weakref

import radical.utils as ru
import utils         as rpu

from .types        import *
from .states       import *
from .exceptions   import *
from .utils        import logger
from .compute_unit import ComputeUnit
from .controller   import UnitManagerController
from .scheduler    import get_scheduler, SCHED_DEFAULT

# TODO: wait queue is on the scheduler

# -----------------------------------------------------------------------------
#
class UnitManager(rpu.Component):
    """A UnitManager manages :class:`radical.pilot.ComputeUnit` instances which
    represent the **executable** workload in RADICAL-Pilot. A UnitManager connects
    the ComputeUnits with one or more :class:`Pilot` instances (which represent
    the workload **executors** in RADICAL-Pilot) and a **scheduler** which
    determines which :class:`ComputeUnit` gets executed on which
    :class:`Pilot`.

    Each UnitManager has a unique identifier :data:`radical.pilot.UnitManager.uid`
    that can be used to re-connect to previoulsy created UnitManager in a
    given :class:`radical.pilot.Session`.

    **Example**::

        s = radical.pilot.Session(database_url=DBURL)

        pm = radical.pilot.PilotManager(session=s)

        pd = radical.pilot.ComputePilotDescription()
        pd.resource = "futuregrid.alamo"
        pd.cores = 16

        p1 = pm.submit_pilots(pd) # create first pilot with 16 cores
        p2 = pm.submit_pilots(pd) # create second pilot with 16 cores

        # Create a workload of 128 '/bin/sleep' compute units
        compute_units = []
        for unit_count in range(0, 128):
            cu = radical.pilot.ComputeUnitDescription()
            cu.executable = "/bin/sleep"
            cu.arguments = ['60']
            compute_units.append(cu)

        # Combine the two pilots, the workload and a scheduler via
        # a UnitManager.
        um = radical.pilot.UnitManager(session=session,
                                   scheduler=radical.pilot.SCHED_ROUND_ROBIN)
        um.add_pilot(p1)
        um.submit_units(compute_units)
    """

    # -------------------------------------------------------------------------
    #
    def __init__(self, session, scheduler=None):
        """Creates a new UnitManager and attaches it to the session.

        **Args:**

            * session (`string`): The session instance to use.

            * scheduler (`string`): The name of the scheduler plug-in to use.

            * input_transfer_workers (`int`): The number of input file transfer
              worker processes to launch in the background.

            * output_transfer_workers (`int`): The number of output file transfer
              worker processes to launch in the background.

        .. note:: `input_transfer_workers` and `output_transfer_workers` can be
                  used to tune RADICAL-Pilot's file transfer performance.
                  However, you should only change the default values if you
                  know what you are doing.
        """
        self._session    = session
        self._components = None
        self._bridges    = None
        self._pilots     = dict()
        self._units      = dict()
        self._batch_id   = 0

        self._uid = ru.generate_id('umgr')
        self._log = ru.get_logger(self.uid, "%s.%s.log" % (session.uid, self._uid))

        self._session.prof.prof('create umgr', uid=self._uid)

        # FIXME: we want to have a config with, for example, wait_poll_timeout.
        #        This config should be defined as config file, not inlined as it
        #        is right now
        cfg = {'wait_poll_timeout' : 0.1 }

        rpu.Component.__init__(self, cfg)

        try:

            cfg = read_json("%s/configs/umgr_%s.json" \
                    % (os.path.dirname(__file_),
                       os.envrion.get('RADICAL_PILOT_UMGR_CONFIG', 'default')))

            if scheduler:
                # overwrite the scheduler from the config file
                cfg['scheduler'] = scheduler

            if not cfg.get('scheduler'):
                # set default scheduler of needed
                cfg['scheduler'] = SCHED_DEFAULT

            bridges    = cfg.get('bridges',    [])
            components = cfg.get('components', [])

            # agent.0 also starts one worker instance for each worker type
            components[rp.UMGR_UPDATE_WORKER]    = 1
            components[rp.UMGR_HEARTBEAT_WORKER] = 1

            # we also need a map from component names to class types
            typemap = {
                rp.UMGR_STAGING_INPUT_COMPONENT  : UMGRStagingInputComponent,
                rp.UMGR_SCHEDULING_COMPONENT     : UMGRSchedulingComponent,
                rp.UMGR_STAGING_OUTPUT_COMPONENT : UMGRStagingOutputComponent,
                rp.UMGR_UPDATE_WORKER            : UMGRUpdateWorker,
                rp.UMGR_HEARTBEAT_WORKER         : UMGRHeartbeatWorker
                }

            self._bridges    = rpu.Component.start_bridges   (bridges)
            self._components = rpu.Component.start_components(components, typemap)

            # FIXME: make sure all communication channels are in place.  This could
            # be replaced with a proper barrier, but not sure if that is worth it...
            time.sleep(1)

            # the command pubsub is used to communicate with the scheduler, and
            # to shut down components, but also to cancel units.  The queue is
            # used to forward submitted units to the scheduler
            self.declare_publisher('command', rp.UMGR_COMMAND_PUBSUB)
            self.declare_output(rp.UMGR_SCHEDULING_PENDING, rp.UMGR_SCHEDULING_QUEUE)

            self._valid = True

        except Exception as e:
            self._log.exception("Agent setup error: %s" % e)
            raise

        self._prof.prof('Agent setup done', logger=self._log.debug)


    # --------------------------------------------------------------------------
    #
    def close(self):
        """
        Shuts down the UnitManager and its components.
        """

        for c in self._components:
            self._log.info("closing component %s", c._name)
            c.close()

        for b in self._bridges:
            self._log.info("closing bridge %s", b._name)
            b.close()

        self._session.prof.prof('closed umgr', uid=self._uid)
        logger.info("Closed UnitManager %s." % str(self._uid))

        self._valid = False


    #--------------------------------------------------------------------------
    #
    @property
    def uid(self):
        """
        Returns the unique id.
        """
        return self._uid


    #--------------------------------------------------------------------------
    #
    @property
    def scheduler(self):
        """
        Returns the scheduler name.
        """

        return self._cfg['scheduler']


    # -------------------------------------------------------------------------
    #
    def add_pilots(self, pilots):
        """
        Associates one or more pilots with the unit manager.  Those pilots are
        then available for the scheduler to place units onto them.

        **Arguments:**

            * **pilots** [:class:`radical.pilot.ComputePilot` or list of
              :class:`radical.pilot.ComputePilot`]: The pilot objects that will be
              added to the unit manager.

        """

        if not self._valid:
            raise RuntimeError("instance is already closed")

        # FIXME: this needs to be picked up the scheduler
        self.publish('command', {'cmd' : 'add_pilots',
                                 'arg' : [x.as_dict for x in ru.tolist(pilots)]})

        for pilot in ru.tolist(pilots):
            self._pilots[pilot.uid] = pilot


    # -------------------------------------------------------------------------
    #
    def list_pilots(self):
        """Lists the UIDs of the pilots currently associated with
        the unit manager.

        **Returns:**

              * A list of :class:`radical.pilot.ComputePilot` UIDs [`string`].
        """
        if not self._valid:
            raise RuntimeError("instance is already closed")

        return self._pilots.keys()


    # -------------------------------------------------------------------------
    #
    def get_pilots(self):
        """get the pilots instances currently associated with
        the unit manager.

        **Returns:**

              * A list of :class:`radical.pilot.ComputePilot` instances.
        """
        if not self._valid:
            raise RuntimeError("instance is already closed")

        return self._pilots.values()


    # -------------------------------------------------------------------------
    #
    def remove_pilots(self, pids, drain=True):
        """Disassociates one or more pilots from the unit manager.

        TODO: Implement 'drain'.

        After a pilot has been removed from a unit manager, it won't process
        any of the unit manager's units anymore. Calling `remove_pilots`
        doesn't stop the pilot itself.

        **Arguments:**

            * **drain** [`boolean`]: Drain determines what happens to the units
              which are managed by the removed pilot(s). If `True`, all units
              currently assigned to the pilot are allowed to finish execution.
              If `False` (the default), then `ACTIVE` units will be canceled.
        """

        if not self._valid:
            raise RuntimeError("instance is already closed")

        # FIXME: this needs to be picked up the scheduler
        self.publish('command', {'cmd' : 'remove_pilots',
                                 'arg' : {'pids'  : [ru.tolist(pids)],
                                          'drain' : drain}})

        for pid in ru.tolist(pids):
            del(self._pilots[pid])


    # -------------------------------------------------------------------------
    #
    def list_units(self):
        """Returns the UIDs of the :class:`radical.pilot.ComputeUnit` managed by
        this unit manager.

        **Returns:**

              * A list of :class:`radical.pilot.ComputeUnit` UIDs [`string`].
        """

        if not self._valid:
            raise RuntimeError("instance is already closed")

        return self._units.keys()


    # -------------------------------------------------------------------------
    #
    def submit_units(self, uds):
        """Submits on or more :class:`radical.pilot.ComputeUnit` instances to the
        unit manager.

        **Arguments:**

            * **uds** [:class:`radical.pilot.ComputeUnitDescription`
              or list of :class:`radical.pilot.ComputeUnitDescription`]: The
              description of the compute unit instance(s) to create.

        **Returns:**

              * A list of :class:`radical.pilot.ComputeUnit` objects.
        """

        if not self._valid:
            raise RuntimeError("instance is already closed")

        # create the units as requested
        units = list()
        for ud in ru.tolist(uds):

            u = ComputeUnit.create(unit_description=ud,
                                   unit_manager_obj=self.uid)
            units.append(u)

            if self._session._rec:
                import radical.utils as ru
                ru.write_json(ud.as_dict(), "%s/%s.batch.%03d.json" \
                        % (self._session._rec, u.uid, self._batch_id))

        if self._session._rec:
            self._batch_id += 1

        # keep a handle on the units
        for unit in units:
            self._units[unit.uid] = unit

        if ru.islist(uds): return units
        else             : return units[0]


    # -------------------------------------------------------------------------
    #
    def get_units(self, uids=None):
        """
        Returns one or more compute units identified by their IDs.

        **Arguments:**

            * **uids** [`string` or `list of strings`]: The IDs of the
              compute unit objects to return.

        **Returns:**

              * A list of :class:`radical.pilot.ComputeUnit` objects.
        """
        if not self._valid:
            raise RuntimeError("instance is already closed")

        if uids:
            if ru.islist(uids):
                return [self._units[x] for x in uids]
            else:
                return self._units[uids]
        else:
            return self._units.values()

    # -------------------------------------------------------------------------
    #
    def wait_units(self, uids=None, stats=[DONE, FAILED, CANCELED], timeout=None):
        """
        Returns when one or more :class:`radical.pilot.ComputeUnits` reach a
        specific state.

        If `unit_uids` is `None`, `wait_units` returns when **all**
        ComputeUnits reach the state defined in `state`.

        **Example**::

            # TODO -- add example

        **Arguments:**

            * **unit_uids** [`string` or `list of strings`]
              If unit_uids is set, only the ComputeUnits with the specified
              uids are considered. If unit_uids is `None` (default), all
              ComputeUnits are considered.

            * **state** [`string`]
              The state that ComputeUnits have to reach in order for the call
              to return.

              By default `wait_units` waits for the ComputeUnits to
              reach a terminal state, which can be one of the following:

              * :data:`radical.pilot.DONE`
              * :data:`radical.pilot.FAILED`
              * :data:`radical.pilot.CANCELED`

            * **timeout** [`float`]
              Timeout in seconds before the call returns regardless of Pilot
              state changes. The default value **None** waits forever.

        **Returns:**
            * a list of states for the units waited upon.
        """

        if not self._valid:
            raise RuntimeError("instance is already closed")

        units  = ru.tolist(self.get_units(uids))
        start  = time.time()
        all_ok = False
        states = list()

        while not all_ok:

            all_ok = True
            states = list()

            for unit in units:
                if unit.state not in ru.tolist(states):
                    all_ok = False
                states.append(unit.state)

            # check timeout
            if (None != timeout) and (timeout <= (time.time() - start)):
                break

            # sleep a little if this cycle was idle
            if not all_ok:
                time.sleep(self._cfg.get('wait_poll_timeout', 0.1)

        # done waiting
        if ru.islist(uids): return states
        else              : return states[0]


    # -------------------------------------------------------------------------
    #
    def cancel_units(self, uids=None):
        """Cancel one or more :class:`radical.pilot.ComputeUnits`.

        **Arguments:**

            * **uids** [`string` or `list of strings`]: The IDs of the
              compute unit objects to cancel.
        """

        if not self._valid:
            raise RuntimeError("instance is already closed")

        # let everybody know...
        cus = ru.tolist(self.get_units(uids))
        for cu in cus:
            self.publish('command', {'cmd' : 'cancel_unit',
                                     'arg' : cu.uids})


    # -------------------------------------------------------------------------
    #
    def register_callback(self, metric=UNIT_STATE, cb, cb_data=None):

        """
        Registers a new callback function with the UnitManager.  Manager-level
        callbacks get called if the specified metric changes.  The default
        metric `UNIT_STATE` fires the callback if any of the ComputeUnits
        managed by the PilotManager change their state.

        All callback functions need to have the same signature::

            def callback_func(obj, value, data)

        where ``object`` is a handle to the object that triggered the callback,
        ``value`` is the metric, and ``data`` is the data provided on
        callback registration..  In the example of `UNIT_STATE` above, the
        object would be the unit in question, and the value would be the new
        state of the unit.

        Available metrics are:

          * `UNIT_STATE`: fires when the state of any of the units which are
            managed by this unit manager instance is changing.  It communicates
            the unit object instance and the units new state.

          * `WAIT_QUEUE_SIZE`: fires when the number of unscheduled units (i.e.
            of units which have not been assigned to a pilot for execution)
            changes.
            FIXME: this is not upported, yet, and should become a cb on the
            umgr_scheduler.
        """

        if metric not in UNIT_MANAGER_METRICS:
            raise ValueError ("Metric '%s' is not available on the unit manager" % metric)

        if metric == UNIT_STATE:
            # ------------------------------------------------------------------
            # unit state callbacks are callbacks on the state pubsub.  We have
            # to do an intermediate step though, to translate callback
            # signatures.  It is somewhat tricky to handle callback data in the
            # two-layer-callback proxying -- sorry for that...
            def state_cb_proxy(topic, msg, state_cb_data):
                assert('topic' == 'state')
                uid   = msg.get('_id')
                unit  = self._units.get(uid)

                if not unit:
                    self._logger.error("cannot proxy unit state cb for '%s'" % uid)
                    return

                _cb      = state_cb_data['cb']
                _cb_data = state_cb_data['cb_data']
                return _cb(unit, unit['state'], _cb_data)
            # ------------------------------------------------------------------

            state_cb_data = {'cb'      : cb,
                             'cb_data' : cb_data}
            self.declare_subscriber('state', UMGR_STATE_PUBSUB,
                    state_cb_proxy, state_cb_data)

        if metric == WAIT_QUEUE_SIZE:
            raise NotImplementedError("Metric '%s' is not yet supported" % metric)

# ------------------------------------------------------------------------------

