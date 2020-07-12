#
# Copyright 2018 PyWren Team
# Copyright IBM Corp. 2019
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
import sys
import time
import textwrap
import pickle
import logging
import tempfile
from pywren_ibm_cloud import utils
from pywren_ibm_cloud.job.partitioner import create_partitions
from pywren_ibm_cloud.utils import is_object_processing_function, sizeof_fmt
from pywren_ibm_cloud.storage.utils import create_func_key, create_agg_data_key
from pywren_ibm_cloud.job.serialize import SerializeIndependent, create_module_data
from pywren_ibm_cloud.config import MAX_AGG_DATA_SIZE, JOBS_PREFIX, STORAGE_BASE_FOLDER
from pywren_ibm_cloud.storage import InternalStorage

import hashlib
import shutil
logger = logging.getLogger(__name__)


def create_map_job(config, internal_storage, executor_id, job_id, map_function, iterdata, runtime_meta,
                   runtime_memory=None, extra_args=None, extra_env=None, obj_chunk_size=None,
                   obj_chunk_number=None, invoke_pool_threads=128, include_modules=[], exclude_modules=[],
                   execution_timeout=None):
    """
    Wrapper to create a map job.  It integrates COS logic to process objects.
    """
    job_created_tstamp = time.time()
    map_func = map_function
    map_iterdata = utils.verify_args(map_function, iterdata, extra_args)
    new_invoke_pool_threads = invoke_pool_threads
    new_runtime_memory = runtime_memory

    if config['pywren'].get('rabbitmq_monitor', False):
        rabbit_amqp_url = config['rabbitmq'].get('amqp_url')
        utils.create_rabbitmq_resources(rabbit_amqp_url, executor_id, job_id)

    # Object processing functionality
    parts_per_object = None
    if is_object_processing_function(map_function):
        # Create partitions according chunk_size or chunk_number
        logger.debug('ExecutorID {} | JobID {} - Calling map on partitions '
                     'from object storage flow'.format(executor_id, job_id))
        map_iterdata, parts_per_object = create_partitions(config, internal_storage,
                                                           map_iterdata, obj_chunk_size,
                                                           obj_chunk_number)
    # ########

    job_description = _create_job(config, internal_storage, executor_id,
                                  job_id, map_func, map_iterdata,
                                  runtime_meta=runtime_meta,
                                  runtime_memory=new_runtime_memory,
                                  extra_env=extra_env,
                                  invoke_pool_threads=new_invoke_pool_threads,
                                  include_modules=include_modules,
                                  exclude_modules=exclude_modules,
                                  execution_timeout=execution_timeout,
                                  job_created_tstamp=job_created_tstamp)

    if parts_per_object:
        job_description['parts_per_object'] = parts_per_object

    return job_description


def create_reduce_job(config, internal_storage, executor_id, reduce_job_id, reduce_function,
                      map_job, map_futures, runtime_meta, reducer_one_per_object=False,
                      runtime_memory=None, extra_env=None, include_modules=[], exclude_modules=[],
                      execution_timeout=None):
    """
    Wrapper to create a reduce job. Apply a function across all map futures.
    """
    job_created_tstamp = time.time()
    iterdata = [[map_futures, ]]

    if 'parts_per_object' in map_job and reducer_one_per_object:
        prev_total_partitons = 0
        iterdata = []
        for total_partitions in map_job['parts_per_object']:
            iterdata.append([map_futures[prev_total_partitons:prev_total_partitons+total_partitions]])
            prev_total_partitons = prev_total_partitons + total_partitions

    reduce_job_env = {'__PW_REDUCE_JOB': True}
    if extra_env is None:
        ext_env = reduce_job_env
    else:
        ext_env = extra_env.copy()
        ext_env.update(reduce_job_env)

    iterdata = utils.verify_args(reduce_function, iterdata, None)

    return _create_job(config, internal_storage, executor_id,
                       reduce_job_id, reduce_function,
                       iterdata, runtime_meta=runtime_meta,
                       runtime_memory=runtime_memory,
                       extra_env=ext_env,
                       include_modules=include_modules,
                       exclude_modules=exclude_modules,
                       execution_timeout=execution_timeout,
                       job_created_tstamp=job_created_tstamp)


def _create_job(config, internal_storage, executor_id, job_id, func, data, runtime_meta,
                runtime_memory=None, extra_env=None, invoke_pool_threads=128, include_modules=[],
                exclude_modules=[], execution_timeout=None, job_created_tstamp=None):
    """
    :param func: the function to map over the data
    :param iterdata: An iterable of input data
    :param extra_env: Additional environment variables for CF environment. Default None.
    :param extra_meta: Additional metadata to pass to CF. Default None.
    :param remote_invocation: Enable remote invocation. Default False.
    :param invoke_pool_threads: Number of threads to use to invoke.
    :param data_all_as_one: upload the data as a single object. Default True
    :param overwrite_invoke_args: Overwrite other args. Mainly used for testing.
    :param exclude_modules: Explicitly keep these modules from pickled dependencies.
    :return: A list with size `len(iterdata)` of futures for each job
    :rtype:  list of futures.
    """
    ext_runtime_storage_config = config.get('ext_runtime', {})
    if ext_runtime_storage_config:
        internal_storage = InternalStorage(ext_runtime_storage_config)

    log_level = os.getenv('PYWREN_LOGLEVEL')

    runtime_name = config['pywren']['runtime']
    if runtime_memory is None:
        runtime_memory = config['pywren']['runtime_memory']

    ext_env = {} if extra_env is None else extra_env.copy()
    if ext_env:
        ext_env = utils.convert_bools_to_string(ext_env)
        logger.debug("Extra environment vars {}".format(ext_env))

    if not data:
        return []

    if execution_timeout is None:
        execution_timeout = config['pywren']['runtime_timeout'] - 5

    job_description = {}
    job_description['runtime_name'] = runtime_name
    job_description['runtime_memory'] = runtime_memory
    job_description['execution_timeout'] = execution_timeout
    job_description['function_name'] = func.__name__
    job_description['extra_env'] = ext_env
    job_description['total_calls'] = len(data)
    job_description['invoke_pool_threads'] = invoke_pool_threads
    job_description['executor_id'] = executor_id
    job_description['job_id'] = job_id

    exclude_modules_cfg = config['pywren'].get('exclude_modules', [])
    include_modules_cfg = config['pywren'].get('include_modules', [])

#    import pdb;pdb.set_trace()

    exc_modules = set()
    inc_modules = set()
    if exclude_modules_cfg:
        exc_modules.update(exclude_modules_cfg)
    if exclude_modules:
        exc_modules.update(exclude_modules)
    if include_modules_cfg is not None:
        inc_modules.update(include_modules_cfg)
    if include_modules_cfg is None and not include_modules:
        inc_modules = None
    if include_modules is not None and include_modules:
        inc_modules.update(include_modules)
    if include_modules is None:
        inc_modules = None

    host_job_meta = {'job_created_tstamp': job_created_tstamp}

    logger.debug('ExecutorID {} | JobID {} - Serializing function and data'.format(executor_id, job_id))
    serializer = SerializeIndependent(runtime_meta['preinstalls'])
    func_and_data_ser, mod_paths = serializer([func] + data, inc_modules, exc_modules)

    module_data = create_module_data(mod_paths)
    data_strs = func_and_data_ser[1:]
    data_bytes, data_ranges = utils.agg_data(data_strs)
    
    # Generate function + data unique identifier
    uuid = _gen_uuid(func_and_data_ser, module_data) 

    _store_data(internal_storage, job_description, data_bytes, data_ranges, uuid, host_job_meta) 
    
    func_str = func_and_data_ser[0]
    func_module_str = pickle.dumps({'func': func_str, 'module_data': module_data}, -1)

    total_size = _validate_size(config, func_module_str, data_bytes, host_job_meta)
    log_msg = ('ExecutorID {} | JobID {} - Uploading function and data '
               '- Total: {}'.format(job_description['executor_id'], job_description['job_id'], total_size))
    print(log_msg) if not log_level else logger.info(log_msg)

    _store_func_and_modules(config, internal_storage, job_description, func_str, func_module_str, uuid, host_job_meta)

    if ext_runtime_storage_config:
        _update_runtime(job_description, module_data, uuid) 

    job_description['metadata'] = host_job_meta
    return job_description

def _validate_size(config, func_module_str, data_bytes, host_job_meta):
    data_size_bytes = len(data_bytes)
    func_module_size_bytes = len(func_module_str)

    total_size = utils.sizeof_fmt(data_size_bytes+func_module_size_bytes)

    host_job_meta['data_size_bytes'] = data_size_bytes
    host_job_meta['func_module_size_bytes'] = func_module_size_bytes

    if 'data_limit' in config['pywren']:
        data_limit = config['pywren']['data_limit']
    else:
        data_limit = MAX_AGG_DATA_SIZE

    if data_limit and data_size_bytes > data_limit*1024**2:
        log_msg = ('ExecutorID {} | JobID {} - Total data exceeded maximum size '
                   'of {}'.format(executor_id, job_id, sizeof_fmt(data_limit*1024**2)))
        raise Exception(log_msg)

    return total_size

def _gen_uuid(func_and_data_ser, module_data):
    return hashlib.md5(func_and_data_ser[0]+func_and_data_ser[1]+pickle.dumps(module_data)).hexdigest()

def _store_data(internal_storage, job_description, data_bytes, data_ranges, uuid, host_job_meta):
    data_key = create_agg_data_key(JOBS_PREFIX, uuid, "")
    
    data_upload_start = time.time() 
#    import pdb;pdb.set_trace()
    internal_storage.put_data(data_key, data_bytes)
    
    data_byte_range = [28, 55]
    range_str = 'bytes={}-{}'.format(*data_byte_range)
    extra_get_args = {}
    extra_get_args['Range'] = range_str
    aaa = internal_storage.get_data(data_key, extra_get_args=extra_get_args)
    data_upload_end = time.time()

    host_job_meta['data_upload_time'] = round(data_upload_end-data_upload_start, 6)
    
    job_description['data_key'] = data_key
    job_description['data_ranges'] = data_ranges

def _store_func_and_modules(config, internal_storage, job_description, func_str, func_module_str, uuid, host_job_meta):
    func_key = create_func_key(JOBS_PREFIX, uuid, "")

    func_upload_start = time.time()

    if config.get('ext_runtime', {}):
        internal_storage.put_func(func_key, func_str)
    else:
        internal_storage.put_func(func_key, func_module_str)

    job_description['func_key'] = func_key
    func_upload_end = time.time()

    host_job_meta['func_upload_time'] = round(func_upload_end - func_upload_start, 6) 

######################################################################################################
# At this point we have the function, modules and data stored under some folder in our filesystem
# The following will be executed only once per runtime version
#
# Pickle function inside /{uuid}/function.pickle and add it to Dockerfile. Update its path in func_key
# Copy unpickled modules to /uuid/modules/ directory to Dockerfile and update mod_key
# Copy unpickled data to /uuid/data directory to Dockerfile and update data_key
# Build docker image and register runtime using build_and_create_runtime function
def _update_runtime(job_description, module_data, uuid):
    from pywren_ibm_cloud.cli.runtime import build_and_create_runtime, get_runtime

    base_docker_image = job_description['runtime_name']
    ext_docker_image = "{}:{}".format(base_docker_image, uuid)

    # update job with new extended runtime name
    job_description['runtime_name'] = ext_docker_image

    # simple check that the image already present in the local docker registry
    # TODO: move higher in the flow, no need to do almost anything if it is there
    # TODO: consider to store extended pywren runtime details in metadata later.
    
#    import pdb;pdb.set_trace()

    runtime = get_runtime(ext_docker_image, memory=job_description['runtime_memory'])
    if 'error' not in runtime:
        return

    #cmd = 'DOCKER_BUILDKIT=1 docker inspect {}'.format(ext_docker_image)
   
    #if os.system(cmd) == 0:
    #    return 

    UUID_DIR = '/'.join([STORAGE_BASE_FOLDER, os.path.dirname(job_description['func_key'])])

    # unpickle pickled modules
    MODULES = '/'.join([UUID_DIR, 'modules'])
    _save_modules(module_data, MODULES)

    ext_docker_file = "{}.{}".format(uuid, 'dockerfile')

    # Generate Dockerfile extended with function dependencies and function
    with open(ext_docker_file, 'w') as df:
            df.write('\n'.join([
                'FROM {}'.format(base_docker_image),
                'ENV PYWREN_TEMP_FOLDER="/"',
                'ENV PYTHONPATH=/{}:${}'.format(MODULES,'PYTHONPATH'), # set python path to point to dependencies folder
                'COPY {} /{}'.format(UUID_DIR, UUID_DIR)
            ]))

#    import pdb;pdb.set_trace()
    # Call pywren cli to build new extended runtime tagged by function hash
    build_and_create_runtime(ext_docker_image, ext_docker_file)

def _inject_in_runtime_docker_image(job_description, func_str, module_data):
    import inspect
    import tempfile
    from distutils.dir_util import copy_tree
    import shutil

    func_uuid = hashlib.md5(func_str).hexdigest()

    base_docker_image = job_description['runtime_name']
    ext_docker_image = "{}:{}".format(base_docker_image, func_uuid)

    # update job with new extended runtime name
    job_description['runtime_name'] = ext_docker_image

    MOD_PATH = 'ext_mod_path'

    # relative pickled function file path
    func_key = '{}/{}.pkl'.format(MOD_PATH, func_uuid)

    # TODO: consider to store extended pywren runtime details in metadata later.

    if os.path.exists(MOD_PATH):
      if os.path.isdir(MOD_PATH):
        shutil.rmtree(MOD_PATH)
      else:
        os.remove(MOD_PATH)

    # Generate per function unique hashsum that will be used as runtime docker image tag
#    f_src = inspect.getsource(func)
#    import pdb;pdb.set_trace()

    ext_docker_file = 'custom.dockerfile'

    # Generate Dockerfile extended with function dependencies and function
    os.makedirs(MOD_PATH, exist_ok=True)
#        for module in mod_paths:
#            if os.path.isdir(module):
#                shutil.copytree(module, '{}/{}'.format(MOD_PATH, os.path.basename(module)))
#            else:
#                shutil.copy2(module, MOD_PATH)

    # save pickled function in the unique file
    with open(func_key, 'wb') as f:
        f.write(func_str)

    # unpickle pickled modules
    _save_modules(module_data, MOD_PATH) 

    with open(ext_docker_file, 'w') as df:
            df.write('\n'.join([
                'FROM {}'.format(base_docker_image),
                'ENV PYTHONPATH=/{}:${}'.format(MOD_PATH,'PYTHONPATH'),
                'COPY {} /{}'.format(MOD_PATH, MOD_PATH),
                'COPY {} /{}'.format(JOBS_PREFIX, JOBS_PREFIX)
            ]))

    # Call pywren cli to build new extended runtime tagged by function hash
    from pywren_ibm_cloud.cli.runtime import build_and_create_runtime
    build_and_create_runtime(ext_docker_image, ext_docker_file)

    job_description['runtime_name'] = ext_docker_image

    # save runtime details to metadata
    # cleanup()
    
    # return the path to the function in the runtime docker image   
    return "/{}".format(func_key)

def _save_modules(module_data, module_path):
    """
    Save modules in the docker image
    """
    from pywren_ibm_cloud.utils import b64str_to_bytes
#    import pdb;pdb.set_trace()    
    if module_data:
        logger.debug("Writing Function dependencies to local disk")
#        os.makedirs(module_path, exist_ok=True)
#        sys.path.append(module_path)

        for m_filename, m_data in module_data.items():
            m_path = os.path.dirname(m_filename)

            if len(m_path) > 0 and m_path[0] == "/":
                m_path = m_path[1:]
            to_make = os.path.join(module_path, m_path)
            try:
                os.makedirs(to_make)
            except OSError as e:
                if e.errno == 17:
                    pass
                else:
                    raise e
            full_filename = os.path.join(to_make, os.path.basename(m_filename))

            with open(full_filename, 'wb') as fid:
                fid.write(b64str_to_bytes(m_data))

        logger.debug("Finished writing Function dependencies")

def clean_job(jobs_to_clean, storage_config, clean_cloudobjects):
    """
    Clean the jobs in a separate process
    """
    with tempfile.NamedTemporaryFile(delete=False) as temp:
        pickle.dump(jobs_to_clean, temp)
        jobs_path = temp.name

    script = """
    from pywren_ibm_cloud.storage import InternalStorage
    from pywren_ibm_cloud.storage.utils import clean_bucket
    from pywren_ibm_cloud.config import JOBS_PREFIX, TEMP_PREFIX
    import pickle
    import os

    storage_config = {}
    clean_cloudobjects = {}
    jobs_path = '{}'
    bucket = storage_config['bucket']

    with open(jobs_path, 'rb') as pk:
        jobs_to_clean = pickle.load(pk)

    internal_storage = InternalStorage(storage_config)
    sh = internal_storage.storage_handler

    for executor_id, job_id in jobs_to_clean:
        prefix = '/'.join([JOBS_PREFIX, executor_id, job_id])
        clean_bucket(sh, bucket, prefix, log=False)
        if clean_cloudobjects:
            prefix = '/'.join([TEMP_PREFIX, executor_id, job_id])
            clean_bucket(sh, bucket, prefix, log=False)

    if os.path.exists(jobs_path):
        os.remove(jobs_path)
    """.format(storage_config, clean_cloudobjects, jobs_path)

    cmdstr = '{} -c "{}"'.format(sys.executable, textwrap.dedent(script))
    os.popen(cmdstr)
