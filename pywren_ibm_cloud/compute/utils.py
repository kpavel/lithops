#
# (C) Copyright IBM Corp. 2018
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
import zipfile
import logging
import pywren_ibm_cloud

logger = logging.getLogger(__name__)


def create_function_handler_zip(zip_location, main_exec_file, backend_location):

    logger.debug("Creating function handler zip in {}".format(zip_location))

    def add_folder_to_zip(zip_file, full_dir_path, sub_dir=''):
        for file in os.listdir(full_dir_path):
            full_path = os.path.join(full_dir_path, file)
            if os.path.isfile(full_path):
                zip_file.write(full_path, os.path.join('pywren_ibm_cloud', sub_dir, file))
            elif os.path.isdir(full_path) and '__pycache__' not in full_path:
                add_folder_to_zip(zip_file, full_path, os.path.join(sub_dir, file))

    try:
        with zipfile.ZipFile(zip_location, 'w', zipfile.ZIP_DEFLATED) as pywren_zip:
            current_location = os.path.dirname(os.path.abspath(backend_location))
            module_location = os.path.dirname(os.path.abspath(pywren_ibm_cloud.__file__))
            main_file = os.path.join(current_location, 'entry_point.py')
            pywren_zip.write(main_file, main_exec_file)
            add_folder_to_zip(pywren_zip, module_location)

    except Exception:
        raise Exception('Unable to create the {} package: {}'.format(zip_location))
