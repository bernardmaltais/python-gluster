#    Copyright 2014, 2015 Sebastien LANGOUREAUX <linuxworkgroup@hotmail.com> and Joe Julian <me@joejulian.name> for the original code
#
#    This file is part of python-gluster.
#
#    python-gluster is free software: you can redistribute it and/or modify
#    it under the terms of the GNU General Public License as published by
#    the Free Software Foundation, either version 3 of the License, or
#    (at your option) any later version.
#
#    python-gluster is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#    GNU General Public License for more details.
#
#    You should have received a copy of the GNU General Public License
#    along with python-gluster.  If not, see <http://www.gnu.org/licenses/>.
#
__author__ = 'Sebastien LANGOUREAUX'


import re
from gluster.peer.ExceptionDetachLocalhost import ExceptionDetachLocalhost
from gluster.peer.ExceptionDetachNotInCluster import ExceptionDetachNotInCluster
from gluster.peer.ExceptionDetachWarning import ExceptionDetachWarning
from gluster.peer.ExceptionProbeWarning import ExceptionProbeWarning
from gluster.peer.ExceptionProbeError import ExceptionProbeError
from gluster.peer.ExceptionProbeLocalhost import ExceptionProbeLocalhost
from gluster.ExceptionGluster import ExceptionGluster


import subprocess


class Peer(object):

    def __init__(self, remote_host = 'localhost'):
        '''
        Constructor
        '''
        self.__remote_host = remote_host


    def probe(self, peer):

        if peer is None or peer == '':
            raise KeyError("You must set the peer to probe")

        try:
            response = subprocess.check_output([
                "/usr/sbin/gluster",
                "peer",
                "probe",
                peer])
            if response == "peer probe: success. Probe on localhost not needed":
                raise ExceptionProbeLocalhost(response)
            if response[:13] == "Probe on host":
                raise ExceptionProbeWarning(response)
            if response[:33] == "Probe returned with unknown errno":
                raise ExceptionProbeError(response)
            return True
        except subprocess.CalledProcessError,e:
            raise ExceptionGluster(e.output)

    def detach(self, peer):

        if peer is None or peer == '':
            raise KeyError("You must set the peer to detach")

        try:
            response = subprocess.check_output([
                "/usr/sbin/gluster",
                "peer",
                "detach",
                peer])
        except subprocess.CalledProcessError,e:
            response = e.output
            if response[-14:] == " is localhost\n":
                raise ExceptionDetachLocalhost(response)
            if response[-24:] == " is not part of cluster\n":
                raise ExceptionDetachNotInCluster(response)
            if response[:23] == "Brick(s) with the peer ":
                raise ExceptionDetachWarning(response)
            if response[:36] == "Detach unsuccessful\nDetach returned ":
                raise ExceptionDetachNotInCluster(response)

            raise ExceptionGluster(e.output)
        return True

    def status(self):
        return self.__status()

    def __status(self, remote_host = None, recursion = False):
        if remote_host is None:
            remote_host = self.__remote_host
        peerstatus = {"host": {},}
        program = ["/usr/sbin/gluster",
            "--remote-host=%s" % remote_host,
            "peer",
            "status"]
        try:
            response = subprocess.check_output(program,stderr=subprocess.STDOUT).split("\n")
        except subprocess.CalledProcessError,e:
            if recursion is False:
                raise ExceptionGluster(e.output)
            else:
                print("Remote host '" + remote_host + "' seems offline")

        # step through the output and build the dict
        for line in response:
            if (line == "No peers present") or (line == "Number of Peers: 0"):
                peerstatus["peers"] = 0
                return peerstatus
            m = re.match("^Number of Peers: (\d+)$", line)
            if m:
                peerstatus["peers"] = int(m.group(1)) + 1
            m = re.match("^Hostname: (.+)$", line)
            if m:
                hostname = m.group(1)
                peerstatus["host"][hostname] = {}
                peerstatus["host"][hostname]["state"] = {}
            m = re.match("Uuid: ([-0-9a-f]+)", line)
            if m:
                peerstatus["host"][hostname]["uuid"] = m.group(1)

        # our first pass through
        if not recursion:
            remotehost = [x for x in
                self.__status(peerstatus["host"].keys()[0],recursion=True)["host"].keys()
                if x not in peerstatus["host"].keys()][0]
            peerstatus["host"][remotehost] = {}
            peerstatus["host"][remotehost]["self"] = True
            peerstatus["host"][remotehost]["state"] = {}
            for host in peerstatus["host"].keys():
                if host != remotehost:
                    peerstatus["host"][remotehost]["uuid"] = self.__status(host,recursion=True)["host"][remotehost]["uuid"]
                remotestatus = self.__status(host,recursion=True)
                for statehost in remotestatus["host"]:
                    for state in remotestatus["host"][statehost]["state"]:
                        peerstatus["host"][statehost]["state"][state] = remotestatus["host"][statehost]["state"][state]
        for line in response:
            m = re.match("^Hostname: (.+)$", line)
            if m:
                hostname = m.group(1)
            m = re.match("State: (.+)", line)
            if m:
                peerstatus["host"][hostname]["state"][remote_host] = m.group(1)

        return peerstatus