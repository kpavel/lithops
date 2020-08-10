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
from pywren_ibm_cloud.config import MAX_AGG_DATA_SIZE, JOBS_PREFIX, STORAGE_FOLDER
from pywren_ibm_cloud.storage import InternalStorage
from pywren_ibm_cloud.cli.runtime import build_runtime, create_runtime, get_runtime
from pywren_ibm_cloud.utils import b64str_to_bytes

import hashlib
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
    data_bytes, data_ranges = utils.agg_data(func_and_data_ser[1:])
    func_str = func_and_data_ser[0]
    
    # Generate function + data unique identifier
    uuid = _gen_uuid(config, func_str, data_bytes, module_data) 

    _store_data(config, internal_storage, job_description, data_bytes, data_ranges, uuid, host_job_meta) 
    
    func_module_str = pickle.dumps({'func': func_str, 'module_data': module_data}, -1)

    total_size = _validate_size(config, job_description, func_module_str, data_bytes, host_job_meta)

    log_msg = ('ExecutorID {} | JobID {} - Uploading function and data '
               '- Total: {}'.format(job_description['executor_id'], job_description['job_id'], total_size))
    print(log_msg) if not log_level else logger.info(log_msg)

    _store_func_and_modules(config, internal_storage, job_description, func_str, module_data, func_module_str, uuid, host_job_meta)

#    if config.get('ext_runtime', {}):
#        _extend_runtime(job_description, uuid) 

    job_description['metadata'] = host_job_meta
    return job_description

def _validate_size(config, job_description, func_module_str, data_bytes, host_job_meta):
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
                   'of {}'.format(job_description['executor_id'], job_description['job_id'], sizeof_fmt(data_limit*1024**2)))
        raise Exception(log_msg)

    return total_size

def _gen_uuid(config, func_str, data_bytes, module_data):
    ext_runtime_config = config.get('ext_runtime', {})
    if ext_runtime_config and ext_runtime_config.get('store_data', None):
        return hashlib.md5(func_str + data_bytes + pickle.dumps(module_data)).hexdigest()
    else:
        return hashlib.md5(func_str + pickle.dumps(module_data)).hexdigest()

def _store_data(config, internal_storage, job_description, data_bytes, data_ranges, uuid, host_job_meta):
    data_key = create_agg_data_key(JOBS_PREFIX, uuid, "")
    
    data_upload_start = time.time() 
    ext_runtime_config = config.get('ext_runtime', {})
    if ext_runtime_config and ext_runtime_config.get('store_data', None):
        internal_storage = InternalStorage(ext_runtime_config)

    internal_storage.put_data(data_key, data_bytes)
    data_upload_end = time.time()

    host_job_meta['data_upload_time'] = round(data_upload_end-data_upload_start, 6)
    
    job_description['data_key'] = data_key
    job_description['data_ranges'] = data_ranges

def _store_func_and_modules(config, internal_storage, job_description, func_str, module_data, func_module_str, uuid, host_job_meta):
    func_key = create_func_key(JOBS_PREFIX, uuid, "")

    func_upload_start = time.time()

    ext_runtime_config = config.get('ext_runtime', {})
    if ext_runtime_config:
        internal_storage = InternalStorage(ext_runtime_config)
        internal_storage.put_func(func_key, func_str)
        modules_path = '/'.join([STORAGE_FOLDER, os.path.dirname(func_key), 'modules'])
        _save_modules(module_data, modules_path)
        job_description['ext_runtime_uuid'] = uuid
    else:
        internal_storage.put_func(func_key, func_module_str)

    job_description['func_key'] = func_key
    func_upload_end = time.time()

    host_job_meta['func_upload_time'] = round(func_upload_end - func_upload_start, 6) 

# If runtime not exists yet, build unique docker image and register runtime
def _extend_runtime(job_description, uuid):
    base_docker_image = job_description['runtime_name']
    ext_docker_image = "{}:{}".format(base_docker_image.split(":")[0], uuid)

    # update job with new extended runtime name
    job_description['runtime_name'] = ext_docker_image

    # simple check that the runtime action already present
    # TODO: move higher in the flow, no need to do almost anything if it is there
    # TODO: consider to store extended pywren runtime details in metadata later.
    runtime = get_runtime(ext_docker_image, memory=job_description['runtime_memory'])
    if 'error' not in runtime:
        return

    runtime_temorary_directory = '/'.join([STORAGE_FOLDER, os.path.dirname(job_description['func_key'])])

    # Unpickle pickled modules
    modules_path = '/'.join([runtime_temorary_directory, 'modules'])
#    _save_modules(module_data, MODULES)

    ext_docker_file = '/'.join([runtime_temorary_directory, "Dockerfile"])

    # Generate Dockerfile extended with function dependencies and function
    with open(ext_docker_file, 'w') as df:
            df.write('\n'.join([
                'FROM {}'.format(base_docker_image),
                'ENV PYTHONPATH={}:${}'.format(modules_path,'PYTHONPATH'), # set python path to point to dependencies folder
                'COPY . {}'.format(runtime_temorary_directory)
            ]))

    # Build new extended runtime tagged by function hash
    cwd = os.getcwd()
    os.chdir(runtime_temorary_directory)
    build_runtime(ext_docker_image, ext_docker_file)
    os.chdir(cwd)
    create_runtime(ext_docker_image)

def _save_modules(module_data, module_path):
    """
    Save modules in the docker image
    """
    if module_data:
        logger.debug("Writing Function dependencies to local disk")

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
