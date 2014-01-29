import sagapilot

DBURL  = "mongodb://ec2-184-72-89-141.compute-1.amazonaws.com:27017"

#-------------------------------------------------------------------------------
#
if __name__ == "__main__":

    try:
        # Create a new session. A session is a set of Pilot Managers
        # and Unit Managers (with associated Pilots and ComputeUnits).
        session = sagapilot.Session(database_url=DBURL)

        print "S UID           : {0} ".format(session.uid)
        print "S Credentials   : {0} ".format(session.list_credentials())
        print "S UnitManagers  : {0} ".format(session.list_unit_managers())
        print "S PilotManagers : {0} ".format(session.list_pilot_managers())

        # Add a Pilot Manager 
        pmgr = sagapilot.PilotManager(session=session)
        print "PM UID          : {0} ".format( pmgr.uid )
        print "PM Pilots       : {0} ".format( pmgr.list_pilots() )

        # Define a 2-core local pilot in /tmp/sagapilot.sandbox that runs 
        # for 10 minutes.
        pdesc = sagapilot.ComputePilotDescription()
        pdesc.resource  = "localhost"
        pdesc.sandbox   = "/tmp/sagapilot.sandbox"
        pdesc.runtime   = 15 # minutes 
        pdesc.cores     = 2 

        # Launch the pilot.
        pilot = pmgr.submit_pilots(pdesc)
        print "Pilot UID       : {0} ".format( pilot.uid )

        # Create a workload of 8 '/bin/sleep' ComputeUnits (tasks)
        compute_units = []

        for unit_count in range(0, 8):
            cu = sagapilot.ComputeUnitDescription()
            cu.environment = {"NAP_TIME" : "10"}
            cu.executable  = "/bin/sleep"
            cu.arguments   = ["$NAP_TIME"]
            cu.cores       = 1
        
            compute_units.append(cu)

        # # Combine the pilot, the workload and a scheduler via 
        # # a UnitManager.
        umgr = sagapilot.UnitManager(session=session, scheduler=sagapilot.SCHED_DIRECT_SUBMISSION)
        umgr.add_pilots(pilot)
        
        umgr.submit_units(compute_units)

        # unit_list = um.list_units()
        # print "* Submitted %s compute units: %s" % (len(unit_list), unit_list)

        # # Wait for all compute units to finish.
        # print "* Waiting for all compute units to finish..."
        umgr.wait_units()

        for unit in umgr.get_units():
            print "UID: {0}, STATE: {1}, START_TIME: {2}, STOP_TIME: {3}".format(
                unit.uid, unit.state, unit.start_time, unit.stop_time)
        
        # Cancel all pilots.
        pmgr.cancel_pilots()

        # Remove session from database
        session.destroy()

    except sagapilot.SagapilotException, ex:
        print "Error: %s" % ex
        
