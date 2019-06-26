import os
import logging
from pywren_ibm_cloud.invoker.ibm_cf.invoker import IBMCloudFunctionsInvoker
from pywren_ibm_cloud.version import __version__
from pywren_ibm_cloud.runtime import create_runtime
from pywren_ibm_cloud.utils import runtime_valid, format_action_name

logger = logging.getLogger(__name__)


class Invoker:

    def __init__(self, config, internal_storage):
        self.log_level = os.getenv('PYWREN_LOG_LEVEL')
        self.config = config
        self.internal_storage = internal_storage

        self.invoker_type = config['pywren']['invoker_backend']

        if self.invoker_type == 'ibm_cf':
            self.invoker_handler = IBMCloudFunctionsInvoker(config)
            self.region = config['ibm_cf']['endpoint'].split('//')[1].split('.')[0]
            self.namespace = config['ibm_cf']['namespace']
            self.invoker_handler_name = 'IBM Cloud Functions'

            log_msg = 'PyWren v{} init for IBM Cloud Functions - Namespace: {} - Region: {}'.format(__version__, self.namespace, self.region)
            logger.info(log_msg)
            if not self.log_level:
                print(log_msg)

        else:
            raise NotImplementedError(("Using {} as internal functions backend is" +
                                       "not supported yet").format(self.backend_type))

        self.runtime_name = self.config['pywren']['runtime']
        self.runtime_memory = self.config['pywren']['runtime_memory']

    def invoke(self, payload):
        return self.invoker_handler.invoke(payload, self.runtime_memory)

    def set_memory(self, memory):
        if memory is not None:
            self.runtime_memory = int(memory)

        log_msg = '{} - Selected Runtime: {} - {}MB'.format(self.invoker_handler_name, self.runtime_name, self.runtime_memory)
        logger.info(log_msg)
        if not self.log_level:
            print(log_msg, end=' ')

        try:
            action_name = format_action_name(self.runtime_name, self.runtime_memory)
            self.runtime_meta = self.internal_storage.get_runtime_info(self.region, self.namespace, action_name)
            if not self.log_level:
                print()

        except Exception:
            logger.debug('{} - Runtime {} with {}MB is not yet installed'.format(self.invoker_handler_name, self.runtime_name, memory))
            if not self.log_level:
                print('(Installing...)')
            create_runtime(self.runtime_name, memory=memory, config=self.config)
            self.runtime_meta = self.internal_storage.get_runtime_info(self.region, self.namespace, action_name)

        if not runtime_valid(self.runtime_meta):
            raise Exception(("The indicated runtime: {} "
                             "is not appropriate for this Python version.")
                            .format(self.runtime_name))

    def get_runtime_preinstalls(self):
        return self.runtime_meta['preinstalls']

    def get_config(self):
        """
        Return config dict
        """
        return self.invoker_handler.config()
