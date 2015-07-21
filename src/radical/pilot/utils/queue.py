
import zmq
import queue
import threading
import multiprocessing

import radical.utils as ru

# defines for queue roles
QUEUE_SOURCE  = 'queue_source'
QUEUE_BRIDGE  = 'queue_bridge'
QUEUE_TARGET  = 'queue_target'
QUEUE_ROLES   = [QUEUE_SOURCE, QUEUE_BRIDGE, QUEUE_TARGET]

# defines for queue types
QUEUE_THREAD  = 'queue_thread'
QUEUE_PROCESS = 'queue_process'
QUEUE_REMOTE  = 'queue_remote'
QUEUE_TYPES   = [QUEUE_THREAD, QUEUE_PROCESS, QUEUE_REMOTE]

# some other local defines
_QUEUE_HWM = 1 # high water mark == buffer size

# some predefined port numbers
_QUEUE_PORTS = {
        'pilot_update_queue'         : 'tcp://*:10000/',
        'pilot_launching_queue'      : 'tcp://*:10002/',
        'unit_update_queue'          : 'tcp://*:10004/',
        'umgr_scheduling_queue'      : 'tcp://*:10006/',
        'umgr_input_staging_queue'   : 'tcp://*:10008/',
        'agent_input_staging_queue'  : 'tcp://*:10010/',
        'agent_scheduling_queue'     : 'tcp://*:10012/',
        'agent_executing_queue'      : 'tcp://*:10014/',
        'agent_output_staging_queue' : 'tcp://*:10016/',
        'umgr_output_staging_queue'  : 'tcp://*:10018/',
        'umgr_input_staging_queue'   : 'tcp://*:10020/'
    }

# --------------------------------------------------------------------------
#
# the source-bridge end of the queue uses a different port than the
# bridge-target end...
#
def _port_inc(address):
    u = ru.Url(address)
    u.port += 1
    return str(u)


# ==============================================================================
#
# Communication between components is done via Queues.  The semantics we expect
# (and which is what is matched by the native Python queue.Queue), is:
#
#   - multiple upstream   components put messages onto the same queue (source)
#   - multiple downstream components get messages from the same queue (target)
#   - local order of messages is maintained
#   - message routing is fair: whatever downstream component calls 'get' first
#     will get the next message (bridge)
#
# The queue implementation we use depends on the participating component types:
# as long as we communicate within threads, treading.Queue can be used.  If
# processes on the same host are involved, we switch to multiprocessing.Queue.
# For remote component processes we use zero-mq (zmq) queues.  In the cases
# where the setup is undetermined, we'll have to use zmq, too, to cater for all
# options.
#
# To make the queue type switching transparent, we provide a set of queue
# implementations and wrappers, which implement the interface of queue.Queue:
#
#   put (item)
#   get ()
#
# Not implemented is, at the moment:
#
#   qsize
#   empty
#   full 
#   put (item, block, timeout
#   put_nowait
#   get (block, timeout)
#   get_nowait
#   task_done
#   join
#
# Our Queue additionally takes 'name', 'role' and 'address' parameter on the
# constructor.  'role' can be 'source', 'bridge' or 'target', where 'source' is
# the sending end of a queue, and 'target' the receiving end, and 'bridge' acts
# as as a message forwarder.  'address' denominates a connection endpoint, and
# 'name' is a unique identifier: if multiple instances in the current process
# space use the same identifier, they will get the same queue instance.  Those
# parameters are obviously mostly useful for the zmq queue.
#
class _QueueRegistry(object):
    
    __metaclass__ = ru.Singleton

    def __init__(self):

        # keep mapping between queue names and instances
        self._lock     = threading.RLock()
        self._registry = {QUEUE_THREAD  : dict(),
                          QUEUE_PROCESS : dict()}


    def get(self, qtype, name, ctor):

        if qtype not in QUEUE_TYPES:
            raise ValueError("no such type '%s'" % qtype)

        with self._lock:
            
            if name in self._registry[qtype]:
                # queue instance for that name exists - return it
                print 'found queue for %s %s' % (qtype, name)
                return self._registry[qtype][name]

            else:
                # queue does not yet exist: create and register
                queue = ctor()
                self._registry[qtype][name] = queue
                print 'created queue for %s %s' % (qtype, name)
                return queue

# create a registry instance
_registry = _QueueRegistry()


# ==============================================================================
#
class Queue(object):
    """
    This is really just the queue interface we want to implement
    """
    def __init__(self, qtype, name, role, address=None):

        self._qtype = qtype
        self._name  = name
        self._role  = role
        self._addr  = address # this could have been an ru.Url


    # --------------------------------------------------------------------------
    #
    # This class-method creates the appropriate sub-class for the Queue.
    #
    @classmethod
    def create(cls, qtype, name, role, address=None):

        # Make sure that we are the base-class!
        if cls != Queue:
            raise TypeError("Queue Factory only available to base class!")

        try:
            impl = {
                QUEUE_THREAD  : QueueThread,
                QUEUE_PROCESS : QueueProcess,
                QUEUE_REMOTE  : QueueRemote,
            }[qtype]
            print 'instantiating %s' % impl
            return impl(qtype, name, role, address)
        except KeyError:
            raise RuntimeError("Queue type '%s' unknown!" % qtype)


    # --------------------------------------------------------------------------
    #
    def put(self, item):
        raise NotImplementedError('put() is not implemented')

    # --------------------------------------------------------------------------
    #
    def get(self):
        raise NotImplementedError('get() is not implemented')


# ==============================================================================
#
class QueueThread(Queue):

    def __init__(self, qtype, name, role, address=None):

        Queue.__init__(self, qtype, name, role, address)
        self._q = _registry.get(qtype, name, queue.Queue)


    # --------------------------------------------------------------------------
    #
    def put(self, item):

        if  self._role == QUEUE_SOURCE:
            self._q.put(item)
        else:
            raise RuntimeError('queue %s (%s) cannot call put()' % \
                    (self._name, self._role))

    # --------------------------------------------------------------------------
    #
    def get(self):
        raise NotImplementedError('get() is not implemented')

        if  self._role == QUEUE_TARGET:
            return self._q.get()
        else:
            raise RuntimeError('queue %s (%s) cannot call get()' % \
                    (self._name, self._role))


# ==============================================================================
#
class QueueProcess(Queue):

    def __init__(self, qtype, name, role, address=None):

        Queue.__init__(self, qtype, name, role, address)
        self._q = _registry.get(qtype, name, multiprocessing.Queue)


    # --------------------------------------------------------------------------
    #
    def put(self, item):

        if  self._role == QUEUE_SOURCE:
            self._q.put(item)
        else:
            raise RuntimeError('queue %s (%s) cannot call put()' % \
                    (self._name, self._role))

    # --------------------------------------------------------------------------
    #
    def get(self):

        if  self._role == QUEUE_TARGET:
            return self._q.get()
        else:
            raise RuntimeError('queue %s (%s) cannot call get()' % \
                    (self._name, self._role))



# ==============================================================================
#
class QueueRemote(Queue):


    def __init__(self, qtype, name, role, address=None):
        """
        This Queue type sets up an zmq channel of this kind:

        source \            / target
                -- bridge -- 
        source /            \ target

        ie. any number of sources can 'zmq.push()' to a bridge (which
        'zmq.pull()'s), and any number of targets can 'zmq.request()' 
        items from the bridge (which 'zmq.response()'s).

        The bridge is the entity which 'bind()'s network interfaces, both source
        and target type endpoints 'connect()' to it.  It is the callees
        responsibility to ensure that only one bridge of a given type exists.

        All component will specify the same address.  Addresses are of the form
        'tcp://ip-number:port/'.  
        
        Note that the address is both used for binding and connecting -- so if
        any component lives on a remote host, all components need to use
        a publicly visible ip number, ie. not '127.0.0.1/localhost'.  The
        bridge-to-target communication needs to use a different port number than
        the source-to-bridge communication.  To simplify setup, we expect
        a single address to be used for all components, and will auto-increase
        the bridge-target port by one.  All given port numbers should be *even*.

        """
        Queue.__init__(self, qtype, name, role, address)

        # sanity check on address
        if not self._addr:  # this may break for ru.Url
            self._addr = _QUEUE_PORTS.get(name)

        if not self._addr:
            raise RuntimeError("no default address found for '%s'" % self._name)

        u = ru.Url(self._addr)
        if  u.path   is not '/'   or \
            u.schema is not 'tcp' :
            raise ValueError("url '%s' cannot be used for remote queues" % u)

        if (u.port % 2):
            raise ValueError("port numbers must be even, not '%d'" % u.port)

        self._addr = str(u)


        # set up the channel
        self._ctx = zmq.Context()

        if self._role == QUEUE_SOURCE:
            self._q       = self._ctx.socket(zmq.PUSH)
            self._q.hwm   = _QUEUE_HWM
            self._q.connect(self._addr)

        elif self._role == QUEUE_BRIDGE:
            self._in      = self._c.socket(zmq.PULL)
            self._in.hwm  = _QUEUE_HWM
            self._in.bind(address)

            self._out     = self._ctx.socket(zmq.REP)
            self._out.hwm = _QUEUE_HWM
            self._out.bind(_port_inc(self._addr))

        elif self._role == QUEUE_TARGET:
            self._q       = self._ctx.socket(zmq.REQ)
            self._q.hwm   = _QUEUE_HWM
            self._q.connect(_port_inc(self._addr))

        else:
            raise RuntimeError ("unsupported queue role '%s'" % self._role)


    # --------------------------------------------------------------------------
    #
    def put(self, item):

        if  self._role == QUEUE_SOURCE:
            self._q.send_json(item)
        else:
            raise RuntimeError('queue %s (%s) cannot call put()' % \
                    (self._name, self._role))

    # --------------------------------------------------------------------------
    #
    def get(self):
        raise NotImplementedError('get() is not implemented')

        if  self._role == QUEUE_TARGET:
            return self._q.recv_json()
        else:
            raise RuntimeError('queue %s (%s) cannot call get()' % \
                    (self._name, self._role))


# ------------------------------------------------------------------------------

