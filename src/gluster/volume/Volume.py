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
import subprocess
import time
from gluster.ExceptionGluster import ExceptionGluster
from gluster.volume.ExceptionVolumeCreate import ExceptionVolumeCreate
from gluster.volume.ExceptionVolumeExtend import ExceptionVolumeExtend
from gluster.volume.ExceptionVolumeQuotaEnable import ExceptionVolumeQuotaEnable
from gluster.volume.ExceptionVolumeQuotaSet import ExceptionVolumeQuotaSet
from gluster.volume.ExceptionVolumeStart import ExceptionVolumeStart



class Volume(object):

    def __init__(self, remote_host = 'localhost'):

        self.__remote_host = remote_host


    def create(self, name, list_bricks, transport = 'tcp', stripe = None, replica = None, quota = None ):
        brickdiv = 1

        if name is None or name == "" :
            raise KeyError("Volume must have a name")
        if list_bricks is None or len(list_bricks) == 0:
            raise KeyError("Volume must have bricks")

        program = ["/usr/sbin/gluster",
            "--remote-host=%s" % self.__remote_host,
            "volume",
            "create",
            name,
            ]

        if stripe is not None :
            stripe = int(stripe)
            program.append("stripe")
            program.append(str(stripe))
            brickdiv = stripe
        if replica is not None:
            replica = int(replica)
            program.append("replica")
            program.append(str(replica))
            brickdiv = brickdiv * replica
        if transport is not None:
            transport = transport if transport in ("tcp","rdma","tcp,rdma","rdma,tcp") else "tcp"
            program.append("transport")
            program.append(transport)
        if len(list_bricks) % brickdiv:
            raise KeyError("Invalid brick count. Bricks must be in multiples of %d" % brickdiv)
        [ program.append(x) for x in list_bricks ]

        try:
            response = subprocess.check_output(program).split("\n")
            success = "Creation of volume %s has been successful. Please start the volume to access data." % name
            if not success in response:
                raise ExceptionVolumeCreate(response)
        except subprocess.CalledProcessError,e:
            raise ExceptionGluster(e.output)

        # We start volume
        program = ["/usr/sbin/gluster",
            "--remote-host=%s" % self.__remote_host,
            "volume",
            "start",
            name,
            ]
        try:
            response = subprocess.check_output(program).split("\n")
            success = "volume start: %s: success" % name
            if not success in response:
                raise ExceptionVolumeStart(response)
        except subprocess.CalledProcessError,e:
            raise ExceptionGluster(e.output)

        #We enable quota if needed
        time.sleep(1)
        if quota is not None:
            program = ["/usr/sbin/gluster",
                "--remote-host=%s" % self.__remote_host,
                "volume",
                "quota",
                name,
                "enable"
                ]
            try:
                response = subprocess.check_output(program).split("\n")
                success = "volume quota : success" % name
                if not success in response:
                    raise ExceptionVolumeQuotaEnable(response)
            except subprocess.CalledProcessError,e:
                raise ExceptionGluster(e.output)

            # We set the quota
            program = ["/usr/sbin/gluster",
                "--remote-host=%s" % self.__remote_host,
                "volume",
                "quota",
                name,
                "limit-usage",
                "/",
                quota,
                "90%"
                ]
            try:
                response = subprocess.check_output(program).split("\n")
                success = "volume quota : success" % name
                if not success in response:
                    raise ExceptionVolumeQuotaSet(response)
            except subprocess.CalledProcessError,e:
                raise ExceptionGluster(e.output)



        return True


    def extend(self, name, list_bricks, replica = None):
        if name is None or name == "" :
            raise KeyError("Volume must have a name")
        if list_bricks is None or len(list_bricks) == 0:
            raise KeyError("Volume must have bricks")

        program = ["/usr/sbin/gluster",
            "--remote-host=%s" % self.__remote_host,
            "volume",
            "add-brick",
            name
        ]

        if replica is not None and replica > 0:
            program.append("replica")
            program.append(replica)

        for brick in list_bricks:
            program.append(brick)

        try:
            response = subprocess.check_output(program).split("\n")
            success = "volume add-brick: success"
            if not success in response:
                raise ExceptionVolumeExtend(response)
        except subprocess.CalledProcessError,e:
            raise ExceptionGluster(e.output)

    def info(self, name = 'all'):

        volumes = {}
        program = ["/usr/sbin/gluster",
            "--remote-host=%s" % self.__remote_host,
            "volume",
            "info",
            name]
        try:
            response = subprocess.check_output(program,stderr=subprocess.STDOUT).split("\n")
        except subprocess.CalledProcessError,e:
            raise ExceptionGluster(e.output)


        for line in response:
            m = re.match("Volume Name: (.+)",line)
            if m:
                volname = m.group(1)
                volumes[volname] = {"bricks": [], "options": {}}
            m = re.match("Type: (.+)",line)
            if m:
                volumes[volname]["type"] = m.group(1)
            m = re.match("Status: (.+)",line)
            if m:
                volumes[volname]["status"] = m.group(1)
            m = re.match("Transport-type: (.+)",line)
            if m:
                volumes[volname]["transport"] = [x.strip() for x in m.group(1).split(",")]
            m = re.match("Brick[1-9][0-9]*: (.+)",line)
            if m:
                volumes[volname]["bricks"].append(m.group(1))
            m = re.match("^([-.a-z]+: .+)$",line)
            if m:
                # TODO this is pretty lazy. Would be better to have the list of all
                #      options and their defaults, then update from matches
                opt,value = [x.strip() for x in m.group(1).split(":")]
                volumes[volname]["options"][opt] = value
        return volumes


