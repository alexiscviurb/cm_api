#!/usr/bin/python
# Licensed to Cloudera, Inc. under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  Cloudera, Inc. licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""
usage: check_cm_service.py [-h] -H HOSTNAME [-p PORT] [-u USERNAME] [--password PASSWORD]
              [--api-version API_VERSION] [--tls] [--version]
A nagios script to monitoring Cloudera Manager Services.
optional arguments:
  -h, --help            show this help message and exit
  -H HOSTNAME, --hostname HOSTNAME
                        The hostname of the Cloudera Manager server.
  -p PORT               The port of the Cloudera Manager server. Defaults to
                        7180 (http) or 7183 (https).
  -u USERNAME, --username USERNAME
                        Login name.
  --password PASSWORD   Login password.
  --api-version API_VERSION
                        API version to be used. Defaults to 16.
  --tls                 Whether to use tls (https).
  -c CLUSTER, --cluster CLUSTER
                        The cluster to monitoring.
  -s SERVICE, --service SERVICE
                        The service to monitoring.
  --version             show program's version number and exit
"""
import argparse
import getpass
import sys

from cm_api.api_client import ApiResource

# Configuration
DEFAULT_HTTP_PORT = 7180
DEFAULT_HTTPS_PORT = 7183
MINIMUM_SUPPORTED_API_VERSION = 12

EXIT_OK = 0
EXIT_WARNING = 1
EXIT_CRITICAL = 2
EXIT_UNKNOWN = 3

NAGIOS_CODE_MESSAGES = {EXIT_OK: "OK",
                        EXIT_WARNING: "WARNING",
                        EXIT_CRITICAL: "CRITICAL",
                        EXIT_UNKNOWN: "UNKNOWN"}

CM_STATE_CODES = {"HISTORY_NOT_AVAILABLE": EXIT_UNKNOWN,
                  "NOT_AVAILABLE": EXIT_UNKNOWN,
                  "DISABLED": EXIT_UNKNOWN,
                  "GOOD": EXIT_OK,
                  "CONCERNING": EXIT_WARNING,
                  "BAD": EXIT_CRITICAL}

# Global API object
api = None

def initialize_api(args):
  """
  Initializes the global API instance using the given arguments.
  @param args: arguments provided to the script.
  """
  global api
  api = ApiResource(server_host=args.hostname, server_port=args.port,
                    username=args.username, password=args.password,
                    version=args.api_version, use_tls=args.use_tls)

def validate_api_compatibility(args):
  """
  Validates the API version.
  @param args: arguments provided to the script.
  """
  if args.api_version and args.api_version < MINIMUM_SUPPORTED_API_VERSION:
    print("ERROR: Given API version: {0}. Minimum supported API version: {1}"
          .format(args.api_version, MINIMUM_SUPPORTED_API_VERSION))


def get_login_credentials(args):
  """
    Gets the login credentials from the user, if not specified while invoking
    the script.
    @param args: arguments provided to the script.
    """
  if not args.username:
    args.username = raw_input("Enter Username: ")
  if not args.password:
    args.password = getpass.getpass("Enter Password: ")

def main():
  """
  The "main" entry that controls the flow of the script based
  on the provided arguments.
  """
  # Parse arguments
  parser = argparse.ArgumentParser(
    description="A utility to interact with AWS using Cloudera Manager.")
  parser.add_argument('-H', '--hostname', action='store', dest='hostname',
                      required=True,
                      help='The hostname of the Cloudera Manager server.')
  parser.add_argument('-p', action='store', dest='port', type=int,
                      help='The port of the Cloudera Manager server. Defaults '
                           'to 7180 (http) or 7183 (https).')
  parser.add_argument('-u', '--username', action='store', dest='username',
                      help='Login name.')
  parser.add_argument('--password', action='store', dest='password',
                      help='Login password.')
  parser.add_argument('--api-version', action='store', dest='api_version',
                      type=int,
                      default=MINIMUM_SUPPORTED_API_VERSION,
                      help='API version to be used. Defaults to {0}.'.format(
                        MINIMUM_SUPPORTED_API_VERSION))
  parser.add_argument('--tls', action='store_const', dest='use_tls',
                      const=True, default=False,
                      help='Whether to use tls (https).')
  parser.add_argument('-c', '--cluster', action='store', dest='cluster',
                      required=True,
                      help='The cluster to monitoring.')
  parser.add_argument('-s', '--service', action='store', dest='service',
                      required=True,
                      help='The service to monitoring.')
  parser.add_argument('--version', action='version', version='%(prog)s 1.0')
  args = parser.parse_args()

  # Use the default port if required.
  if not args.port:
    if args.use_tls:
      args.port = DEFAULT_HTTPS_PORT
    else:
      args.port = DEFAULT_HTTP_PORT

  validate_api_compatibility(args)
  get_login_credentials(args)
  initialize_api(args)


#########################################

  c = api.get_cluster(args.cluster)
  s = c.get_service(args.service)
  
  summary = s.healthSummary
  check = s.healthChecks

  if s.healthSummary == "CONCERNING":
    status = "%s: %s is %s" % (NAGIOS_CODE_MESSAGES[CM_STATE_CODES["GOOD"]], s.name, summary)
    code = CM_STATE_CODES["GOOD"]
  else:
    status = "%s: %s is %s" % (NAGIOS_CODE_MESSAGES[CM_STATE_CODES[summary]], s.name, summary)
    code = CM_STATE_CODES[summary]

  if s.healthSummary != "GOOD" and s.healthSummary != "DISABLED":
    status = status + " - "
    for chk in check:
      if chk['summary'] != "GOOD" and chk['summary'] != "DISABLED":
        status = status + "%s (%s) " % (chk['name'],chk['summary'])

  return (status, code)

#########################################

if __name__ == "__main__":
  print main()[0]
  sys.exit(main()[1])
