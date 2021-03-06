#!/usr/bin/env python

__copyright__ = 'Copyright 2013-2014, http://radical.rutgers.edu'
__license__   = 'MIT'

import os
import sys

os.environ['RADICAL_PILOT_VERBOSE'] = 'REPORT'
os.environ['RADICAL_PILOT_PROFILE'] = 'TRUE'

import radical.pilot as rp
import radical.utils as ru


# ------------------------------------------------------------------------------
#
# READ the RADICAL-Pilot documentation: http://radicalpilot.readthedocs.org/
#
# ------------------------------------------------------------------------------


#------------------------------------------------------------------------------
#
if __name__ == '__main__':

    # we use a reporter class for nicer output
    report = ru.LogReporter(name='radical.pilot')
    report.title('Getting Started (RP version %s)' % rp.version)

    # use the resource specified as argument, fall back to localhost
    if   len(sys.argv)  > 2: report.exit('Usage:\t%s [resource]\n\n' % sys.argv[0])
    elif len(sys.argv) == 2: resource = sys.argv[1]
    else                   : resource = 'local.localhost'

    # Create a new session. No need to try/except this: if session creation
    # fails, there is not much we can do anyways...
    session = rp.Session()

    # all other pilot code is now tried/excepted.  If an exception is caught, we
    # can rely on the session object to exist and be valid, and we can thus tear
    # the whole RP stack down via a 'session.close()' call in the 'finally'
    # clause...
    try:

        # read the config used for resource details
        report.info('read config')
        config = ru.read_json('%s/../config.json' % os.path.dirname(os.path.abspath(__file__)))
        report.ok('>>ok\n')

        report.header('submit pilots')

        # Add a Pilot Manager. Pilot managers manage one or more ComputePilots.
        pmgr = rp.PilotManager(session=session)

        # Define an [n]-core local pilot that runs for [x] minutes
        # Here we use a dict to initialize the description object
        report.info('create pilot description')
        pd_init = {
                'resource'      : resource,
                'cores'         : 64,  # pilot size
                'runtime'       : 15,  # pilot runtime (min)
                'exit_on_error' : True,
                'project'       : config[resource]['project'],
                'queue'         : config[resource]['queue'],
                'access_schema' : config[resource]['schema']
                }
        pdesc = rp.ComputePilotDescription(pd_init)
        report.ok('>>ok\n')

        # Launch the pilot.
        pilot = pmgr.submit_pilots(pdesc)


        report.header('submit units')

        # Register the ComputePilot in a UnitManager object.
        umgr = rp.UnitManager(session=session)
        umgr.add_pilots(pilot)

        # Create a workload of ComputeUnits.
        # Each compute unit runs '/bin/date'.

        n = 128   # number of units to run
        report.info('create %d unit description(s)\n\t' % n)

        cuds = list()
        for i in range(0, n):

            # create a new CU description, and fill it.
            # Here we don't use dict initialization.
            cud = rp.ComputeUnitDescription()
            # trigger an error now and then
            if not i % 10: cud.executable = '/bin/data' # does not exist
            else         : cud.executable = '/bin/date'

            cuds.append(cud)
            report.progress()
        report.ok('>>ok\n')

        # Submit the previously created ComputeUnit descriptions to the
        # PilotManager. This will trigger the selected scheduler to start
        # assigning ComputeUnits to the ComputePilots.
        units = umgr.submit_units(cuds)

        # Wait for all compute units to reach a final state (DONE, CANCELED or FAILED).
        report.header('gather results')
        umgr.wait_units()
    
        report.info('\n')
        for unit in units:
            if unit.state == rp.FAILED:
                report.plain('  * %s: %s, exit: %3s, err: %s' \
                        % (unit.uid, unit.state[:4], 
                           unit.exit_code, unit.stderr.strip()[-35:]))
                report.error('>>err\n')
            else:
                report.plain('  * %s: %s, exit: %3s, out: %s' \
                        % (unit.uid, unit.state[:4], 
                            unit.exit_code, unit.stdout.strip()[:35]))
                report.ok('>>ok\n')
    

    except Exception as e:
        # Something unexpected happened in the pilot code above
        report.error('caught Exception: %s\n' % e)
        raise

    except (KeyboardInterrupt, SystemExit) as e:
        # the callback called sys.exit(), and we can here catch the
        # corresponding KeyboardInterrupt exception for shutdown.  We also catch
        # SystemExit (which gets raised if the main threads exits for some other
        # reason).
        report.warn('exit requested\n')

    finally:
        # always clean up the session, no matter if we caught an exception or
        # not.  This will kill all remaining pilots.
        report.header('finalize')
        session.close(cleanup=False)

    report.header()


    # fetch profiles and convert into inspectable data frames
    if not session:
        sys.exit(-1)


    import radical.pilot.utils as rpu

    # we have a session
    sid        = session.uid
    profiles   = rpu.fetch_profiles(sid=sid, tgt='/tmp/')
    profile    = rpu.combine_profiles (profiles)
    frame      = rpu.prof2frame(profile)
    sf, pf, uf = rpu.split_frame(frame)

  # print len(sf)
  # print len(pf)
  # print len(uf)
  # 
  # print sf[0:10]
  # print pf[0:10]
  # print uf[0:10]
    rpu.add_info(uf)

    for index, row in uf.iterrows():
        if str(row['info']) != 'nan':
            print "%-20s : %-10s : %-25s : %-20s" % \
                    (row['time'], row['uid'], row['state'], row['info'])

    rpu.add_states(uf)
    adv = uf[uf['event'].isin(['advance'])]
    print len(adv)
  # print uf[uf['uid'] == 'unit.000001']
  # print list(pf['event'])

    rpu.add_frequency(adv, 'f_exe', 0.5, {'state' : 'Executing', 'event' : 'advance'})
    print adv[['time', 'f_exe']].dropna(subset=['f_exe'])

    s_frame, p_frame, u_frame = rpu.get_session_frames(sid)
    print str(u_frame)

    for f in profiles:
        os.unlink(f)


#-------------------------------------------------------------------------------

