"""
.. module:: sinon.mpworker.pilotlauncher
   :platform: Unix
   :synopsis: Implements the pilot laumcher functionality.

.. moduleauthor:: Ole Weidner <ole.weidner@rutgers.edu>
"""

__copyright__ = "Copyright 2013-2014, http://radical.rutgers.edu"
__license__ = "MIT"

import os
import time
import saga
import datetime
import traceback

from radical.utils import which

from sagapilot import states
from sagapilot.credentials import SSHCredential
from sagapilot.utils.logger import logger


# ------------------------------------------------------------------------
#
def launch_pilot(pilot_uid, pilot_description,
                 resource_cfg, session_dict, credentials_dict):
    """launch_pilot() is a self contained function that launches a SAGA-Pilot
    agent on a local or remote machine according to the provided specification.

    This function is called asynchronously and attached to one of the
    PilotManager's worker processes. To maintain pickleability et al., no
    facade objects etc. go in an out of this function, just plain dictionaries.
    """

    #resource_key = pilot_description['description']['Resource']
    number_cores = pilot_description['Cores']
    runtime = pilot_description['Runtime']
    queue = pilot_description['Queue']
    sandbox = pilot_description['Sandbox']

    # At the end of the submission attempt, pilot_logs will contain
    # all log messages.
    pilot_logs = []

    ########################################################
    # Create SAGA Job description and submit the pilot job #
    ########################################################
    try:
        # create a custom SAGA Session and add the credentials
        # that are attached to the session
        saga_session = saga.Session()

        for cred_dict in credentials_dict:
            cred = SSHCredential.from_dict(cred_dict)

            saga_session.add_context(cred._context)

            logger.debug("Added credential %s to SAGA job service." % str(cred))

        # Create working directory if it doesn't exist and copy
        # the agent bootstrap script into it.
        #
        # We create a new sub-driectory for each agent. each
        # agent will bootstrap its own virtual environment in this
        # directory.
        #
        fs = saga.Url(resource_cfg['filesystem'])
        if sandbox is not None:
            fs.path += sandbox
        else:
            # No sandbox defined. try to determine
            found_dir_success = False

            if resource_cfg['filesystem'].startswith("file"):
                workdir = os.path.expanduser("~")
                found_dir_success = True
            else:
                # A horrible hack to get the home directory on the
                # remote machine.
                import subprocess

                usernames = [None]
                for cred in credentials:
                    usernames.append(cred["user_id"])

                # We have mutliple usernames we can try... :/
                for username in usernames:
                    if username is not None:
                        url = "%s@%s" % (username, fs.host)
                    else:
                        url = fs.host

                    p = subprocess.Popen(
                        ["ssh", url,  "pwd"],
                        stdout=subprocess.PIPE, stderr=subprocess.PIPE
                    )
                    workdir, err = p.communicate()

                    if err != "":
                        logger.warning("Couldn't determine remote working directory for %s: %s" % (url, err))
                    else:
                        logger.debug("Determined remote working directory for %s: %s" % (url, workdir))
                        found_dir_success = True
                        break

            if found_dir_success is False:
                error_msg = "Couldn't determine remote working directory."
                logger.error(error_msg)
                raise Exception(error_msg)

            # At this point we have determined 'pwd'
            fs.path += "%s/sagapilot.sandbox" % workdir.rstrip()

        # This is the base URL / 'sandbox' for the pilot! 
        agent_dir_url = saga.Url("%s/pilot-%s/" % (str(fs), str(pilot_uid)))

        agent_dir = saga.filesystem.Directory(
            agent_dir_url,
            saga.filesystem.CREATE_PARENTS)

        agent_dir.close()

        log_msg = "Created agent sandbox '%s'." % str(agent_dir_url)
        pilot_logs.append(log_msg)
        logger.debug(log_msg)

        # Copy the bootstrap shell script
        # This works for installed versions of saga-pilot
        bs_script = which('bootstrap-and-run-agent')
        if bs_script is None:
            bs_script = os.path.abspath("%s/../../../bin/bootstrap-and-run-agent" % os.path.dirname(os.path.abspath(__file__)))
        # This works for non-installed versions (i.e., python setup.py test)
        bs_script_url = saga.Url("file://localhost/%s" % bs_script)

        bs_script = saga.filesystem.File(bs_script_url)

        bs_script.copy(agent_dir_url)

        bs_script.close()

        log_msg = "Copied '%s' script to agent sandbox." % bs_script_url
        pilot_logs.append(log_msg)
        logger.debug(log_msg)

        # Copy the agent script
        cwd = os.path.dirname(os.path.abspath(__file__))
        agent_path = os.path.abspath("%s/../agent/sagapilot-agent.py" % cwd)
        agent_script_url = saga.Url("file://localhost/%s" % agent_path)
        agent_script = saga.filesystem.File(agent_script_url)
        agent_script.copy(agent_dir_url)

        agent_script.close()

        log_msg = "Copied '%s' script to agent sandbox." % agent_script_url
        pilot_logs.append(log_msg)
        logger.debug(log_msg)

        # extract the required connection parameters and uids
        # for the agent:
        database_host = session_dict["database_url"].split("://")[1]
        database_name = session_dict["database_name"]
        session_uid = session_dict["uid"]

        # now that the script is in place and we know where it is,
        # we can launch the agent
        js = saga.job.Service(resource_cfg['URL'], session=saga_session)

        jd = saga.job.Description()
        jd.working_directory = agent_dir_url.path
        jd.executable = "./bootstrap-and-run-agent"
        jd.arguments = ["-r", database_host,   # database host (+ port)
                        "-d", database_name,   # database name
                        "-s", session_uid,     # session uid
                        "-p", str(pilot_uid),  # pilot uid
                        "-t", runtime,         # agent runtime in minutes
                        "-c", number_cores,    # number of cores
                        "-C"]                  # clean up by default

        if 'task_launch_mode' in resource_cfg:
            jd.arguments.extend(["-l", resource_cfg['task_launch_mode']])

        # process the 'queue' attribute
        if queue is not None:
            jd.queue = queue
        elif 'default_queue' in resource_cfg:
            jd.queue = resource_cfg['default_queue']

        # if resource config defines 'pre_bootstrap' commands,
        # we add those to the argument list
        if 'pre_bootstrap' in resource_cfg:
            for command in resource_cfg['pre_bootstrap']:
                jd.arguments.append("-e \"%s\"" % command)

        # if resourc configuration defines a custom 'python_interpreter',
        # we add it to the argument list
        if 'python_interpreter' in resource_cfg:
            jd.arguments.append(
                "-i %s" % resource_cfg['python_interpreter'])

        jd.output = "STDOUT"
        jd.error = "STDERR"
        jd.total_cpu_count = number_cores
        jd.wall_time_limit = runtime

        pilotjob = js.create_job(jd)
        pilotjob.run()

        pilotjob_id = pilotjob.id

        js.close()

        log_msg = "ComputePilot agent successfully submitted with JobID '%s'" % pilotjob_id
        pilot_logs.append(log_msg)
        logger.info(log_msg)

        # Submission was successful. We can set the pilot state to 'PENDING'.

        result = {
            "pilot_uid":   str(pilot_uid),
            "saga_job_id": pilotjob_id,
            "state":       states.PENDING,
            "sandbox":     str(agent_dir_url),
            "submitted":   datetime.datetime.utcnow(),
            "logs":        pilot_logs
        }
        return result

        # self._db.update_pilot_state(
        #     pilot_uid=str(pilot_uid),
        #     state=states.PENDING,
        #     sagajobid=pilotjob_id,
        #     sandbox=str(agent_dir_url),
        #     submitted=datetime.datetime.utcnow(),
        #     logs=pilot_logs)

    except Exception, ex:
        error_msg = "Pilot Job submission failed:\n %s" % (
            traceback.format_exc())
        pilot_logs.append(error_msg)
        logger.error(error_msg)

        result = {
            "pilot_uid":   str(pilot_uid),
            "saga_job_id": pilotjob_id,
            "state":       states.FAILED,
            "sandbox":     str(agent_dir_url),
            "submitted":   datetime.datetime.utcnow(),
            "logs":        pilot_logs
        }
        return result

        # Submission wasn't successful. Update the pilot's state to 'FAILED'.
        # self._db.update_pilot_state(pilot_uid=str(pilot_uid),
        #                             state=states.FAILED,
        #                             submitted=now,
        #                             logs=pilot_logs)
