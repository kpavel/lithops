import pywren_ibm_cloud as pywren
#import couchdb
#import dataclay

def hello(name):
#    print(dataclay)
    print("aaa") 
#    print(couchdb.Server)
#    print("couchdb version {}".format(couchdb.__version__))
    return 'Hello 4224 {} !'.format(name)

#pw = pywren.ibm_cf_executor(runtime='kpavel/pywren-test')
pw = pywren.ibm_cf_executor()
#import pdb;pdb.set_trace()

pw.map(hello, ["aaa0", "bbb1", "ccc2", "ddd3", "eee4", "fff5", "ggg6", "hhh7", "iii8", "jjj9", "aaa0", "bbb1", "ccc2", "ddd3", "eee4", "fff5", "ggg6", "hhh7", "iii8", "jjj9", "aaa0", "bbb1", "ccc2", "ddd3", "eee4", "fff5", "ggg6", "hhh7", "iii8", "jjj9"])
#pw.call_async(hello, 'World', extra_env = {"FOO": 'pywren_ibm_cloud.test4.hello'})
print(pw.get_result())
