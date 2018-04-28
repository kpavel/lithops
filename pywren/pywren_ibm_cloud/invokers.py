#
# Copyright 2018 PyWren Team
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import os
from pywren_ibm_cloud.cf_connector import CloudFunctions

SOURCE_DIR = os.path.dirname(os.path.abspath(__file__))


class IBMCloudFunctionsInvoker(object):
    
    def __init__(self, config):
        self.namespace = config['namespace']
        self.endpoint = config['endpoint']
        self.pw_action_name = config['action_name']
        self.client = CloudFunctions(config)

        self.TIME_LIMIT = True

    def invoke(self, payload):
        """
        Invoke -- return information about this invocation
        """
        activation_id = self.client.invoke(action_name=self.pw_action_name,
                                           payload=payload,
                                           invocation_type='Event')
        return activation_id

    def config(self):
        """
        Return config dict
        """
        return {'cf_action_name' : self.pw_action_name,
                'cf_namespace' : self.namespace,
                'cf_endpoint' : self.endpoint}