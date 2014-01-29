"""Pilot Manager tests
"""

import sinon
import unittest

import uuid
from copy import deepcopy
from sagapilot.db import Session
from pymongo import MongoClient

DBURL  = 'mongodb://ec2-184-72-89-141.compute-1.amazonaws.com:27017/'
DBNAME = 'sinon_test'

#-----------------------------------------------------------------------------
#
class Test_PilotManager(unittest.TestCase):
    # silence deprecation warnings under py3

    def setUp(self):
        # clean up fragments from previous tests
        client = MongoClient(DBURL)
        client.drop_database(DBNAME)

    def tearDown(self):
        # clean up after ourselves 
        client = MongoClient(DBURL)
        client.drop_database(DBNAME)

    def failUnless(self, expr):
        # St00pid speling.
        return self.assertTrue(expr)

    def failIf(self, expr):
        # St00pid speling.
        return self.assertFalse(expr)

    #-------------------------------------------------------------------------
    #
    def test__pilotmanager_create(self):
        """ Test if pilot manager creation works as expected.
        """
        session = sinon.Session(database_url=DBURL, database_name=DBNAME)

        assert session.list_pilot_managers() == [], "Wrong number of pilot managers"

        pm = sinon.PilotManager(session=session)
        assert session.list_pilot_managers() == [pm.uid], "Wrong list of pilot managers"

        pm = sinon.PilotManager(session=session)
        assert len(session.list_pilot_managers()) == 2, "Wrong number of pilot managers"


    #-------------------------------------------------------------------------
    #
    def test__pilotmanager_reconnect(self):
        """ Test if pilot manager re-connect works as expected.
        """
        session = sinon.Session(database_url=DBURL, database_name=DBNAME)

        pm = sinon.PilotManager(session=session)
        assert session.list_pilot_managers() == [pm.uid], "Wrong list of pilot managers"

        pm_r = session.get_pilot_managers(pilot_manager_ids=pm.uid)

        assert session.list_pilot_managers() == [pm_r.uid], "Wrong list of pilot managers"

        assert pm.uid == pm_r.uid, "Pilot Manager IDs not matching!"

    #-------------------------------------------------------------------------
    #
    def test__pilotmanager_list_pilots(self):
        """ Test if listing pilots works as expected.
        """
        session = sinon.Session(database_url=DBURL, database_name=DBNAME)

        pm1 = sinon.PilotManager(session=session)
        assert len(pm1.list_pilots()) == 0, "Wrong number of pilots returned."

        pm2 = sinon.PilotManager(session=session)
        assert len(pm2.list_pilots()) == 0, "Wrong number of pilots returned."

        for i in range(0, 2):
            cpd = sinon.ComputePilotDescription()
            cpd.resource          = "localhost"
            cpd.cores             = 1
            cpd.run_time          = 1
            cpd.working_directory = "/tmp/sagapilot.sandbox.unittests"

            pm1.submit_pilots(pilot_descriptions=cpd)
            pm2.submit_pilots(pilot_descriptions=cpd)

        assert len(pm1.list_pilots()) == 2, "Wrong number of pilots returned."
        assert len(pm2.list_pilots()) == 2, "Wrong number of pilots returned."

    #-------------------------------------------------------------------------
    #
    def test__pilotmanager_list_pilots_after_reconnect(self):
        """ Test if listing pilots after a reconnect works as expected.
        """
        session = sinon.Session(database_url=DBURL, database_name=DBNAME)

        pm1 = sinon.PilotManager(session=session)
        assert len(pm1.list_pilots()) == 0, "Wrong number of pilots returned."

        pm2 = sinon.PilotManager(session=session)
        assert len(pm2.list_pilots()) == 0, "Wrong number of pilots returned."

        for i in range(0, 2):
            cpd = sinon.ComputePilotDescription()
            cpd.resource          = "localhost"
            cpd.cores             = 1
            cpd.run_time          = 1
            cpd.working_directory = "/tmp/sagapilot.sandbox.unittests"

            pm1.submit_pilots(pilot_descriptions=cpd)
            pm2.submit_pilots(pilot_descriptions=cpd)

        assert len(pm1.list_pilots()) == 2, "Wrong number of pilots returned."
        assert len(pm2.list_pilots()) == 2, "Wrong number of pilots returned."

        pm1_r = session.get_pilot_managers(pilot_manager_ids=pm1.uid)
        pm2_r = session.get_pilot_managers(pilot_manager_ids=pm2.uid)

        assert len(pm1_r.list_pilots()) == 2, "Wrong number of pilots returned."
        assert len(pm2_r.list_pilots()) == 2, "Wrong number of pilots returned."


    #-------------------------------------------------------------------------
    #
    def test__pilotmanager_get_pilots(self):
        session = sinon.Session(database_url=DBURL, database_name=DBNAME)

        pm1 = sinon.PilotManager(session=session)
        assert len(pm1.list_pilots()) == 0, "Wrong number of pilots returned."

        pm2 = sinon.PilotManager(session=session)
        assert len(pm2.list_pilots()) == 0, "Wrong number of pilots returned."

        pm1_pilot_uids = []
        pm2_pilot_uids = []

        for i in range(0, 2):
            cpd = sinon.ComputePilotDescription()
            cpd.resource          = "localhost"
            cpd.cores             = 1
            cpd.run_time          = 1
            cpd.working_directory = "/tmp/sagapilot.sandbox.unittests"

            pilot_pm1 = pm1.submit_pilots(pilot_descriptions=cpd)
            pm1_pilot_uids.append(pilot_pm1.uid)

            pilot_pm2 = pm2.submit_pilots(pilot_descriptions=cpd)
            pm2_pilot_uids.append(pilot_pm2.uid)

        for i in pm1.list_pilots():
            pilot = pm1.get_pilots(i)
            assert pilot[0].uid in pm1_pilot_uids, "Wrong pilot ID %s (not in %s)" % (pilot[0].uid, pm1_pilot_uids)


        assert len(pm1.get_pilots()) == 2, "Wrong number of pilots."

        for i in pm2.list_pilots():
            pilot = pm2.get_pilots(i)
            assert pilot[0].uid in pm2_pilot_uids, "Wrong pilot ID %s" % pilot[0].uid

        assert len(pm2.get_pilots()) == 2, "Wrong number of pilots."