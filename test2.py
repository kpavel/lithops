import pywren_ibm_cloud as pywren
import couchdb
import dataclay

def hello(name):
    print(couchdb)
    print(dataclay)
    return 'Hello 224 {}!'.format(name)

#pw = pywren.ibm_cf_executor(runtime='kpavel/pywren-test')
pw = pywren.ibm_cf_executor()
#import pdb;pdb.set_trace()

#pw.map(hello, ["aaa", "bbb"], extra_env = {'FOO': 'test3.hello'})
pw.call_async(hello, 'World', extra_env = {"FOO": 'pywren_ibm_cloud.test4.hello'})
print(pw.get_result())
