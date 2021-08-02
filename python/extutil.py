import time
import json
import datetime

def remove_none_attributes(payload):
    """Assumes dict"""
    return {k: v for k, v in payload.items() if not v is None}

def current_epoch_time_usec_num():
    return int(time.time() * 1000000)

def account_context(context):
    vals = context.invoked_function_arn.split(':')
    return {
        "number": vals[4],
        "region": vals[3]
    }

def gen_log(title, details, is_error=False):
    return {
        "title": title,
        "details": details,
        "timestamp_usec": current_epoch_time_usec_num(),
        "is_error": is_error
    }

def defaultconverter(o):
    if isinstance(o, datetime.datetime):
        return o.__str__()

def creturn(status_code, progress, success=None, error=None, logs=None, pass_back_data=None, state=None, props=None, links=None, callback_sec=2, error_details={}):
    
    assembled = remove_none_attributes({
        "statusCode": 200,
        "progress": progress,
        "success": success,
        "error": error,
        "error_details": error_details,
        "pass_back_data": pass_back_data,
        "state": state,
        "props": props,
        "links": links,
        "logs": logs,
        "callback_sec":callback_sec
    })
    print(f'assembled = {assembled}')

    return json.loads(json.dumps(assembled, default=defaultconverter))

def sort_f(td):
    return td['timestamp_usec']

class ExtensionHandler:

    def refresh(self):
        self.logs = []
        self.ops = {}
        self.retries = {}
        self.ret = False
        self.callback_sec = 0
        self.status_code = None
        self.progress = None
        self.success = None
        self.error = None
        self.props = None
        self.links = None
        self.callback = None
        self.error_details = None
    
    def __init__(self):
        self.refresh()    
        
    def declare_pass_back_data(self, pass_back_data):
        print(f"setting pass_back_data")
        self.ops = pass_back_data.get('ops') or {}
        self.retries = pass_back_data.get('retries') or {}
        print(f"Ops = {self.ops}, retries = {self.retries}")
        
    def add_op(self, opkey, opvalue=True):
        print(f'add op {opkey} with avlue {opvalue}')
        self.ops[opkey] = opvalue
        
    def complete_op(self, opkey):
        print(f'completing op {opkey}')
        try:
            _ = self.ops.pop(opkey)
        except:
            pass
        
    def add_log(self, title, details={}, is_error=False):
        print(f'Adding Log with title {title}, error = {is_error}')
        self.logs.append(gen_log(title, details, is_error))

    def declare_return(self, status_code, progress, success=None, props=None, links=None, error_code=None, error_details=None, callback=True, callback_sec=0):
        print(f"success = {success}, error_code = {error_code}")
        self.status_code = status_code
        self.progress = progress
        self.success = success
        self.error = error_code
        self.props = props
        self.links = links
        self.callback = callback
        self.callback_sec = callback_sec
        self.error_details = error_details
        self.ret = True
        
    def finish(self):
        pass_back_data = {}
        if self.error:
            pass_back_data['ops'] = self.ops
            pass_back_data['retries'] = self.retries
            this_retries = pass_back_data['retries'].get(self.error, 0) + 1
            pass_back_data['retries'][self.error] = this_retries
            if this_retries < 15 and self.callback:
                self.error = None
                self.error_details = None
                if not self.callback_sec:
                    self.callback_sec = 2**this_retries                

        elif not self.success:
            self.error = "no_success_or_error"
            self.error_details = {"error": "Finish was called without either success or an error code being passed."}

#       self.logs.sort(key=sort_f, reverse=True)
            
        return creturn(
            self.status_code, self.progress, self.success, self.error, self.logs, 
            pass_back_data, None, self.props, self.links, self.callback_sec, self.error_details
        )
    

# A decorator is a function that expects ANOTHER function as parameter
def ext(f=None, handler=None, op=None):
    import functools
    
    if not f:
        return functools.partial(
            ext,
            handler=handler,
            op=op
        )

    if not handler:
        raise Exception(f"Must pass handler of type ExtensionHandler to ext decorator")

    @functools.wraps(f)
    def the_wrapper_around_the_original_function(*args, **kwargs):
        try:
            if handler.ret:
                return None
            elif op and op not in handler.ops.keys():
                return None
        except:
            raise Exception(f"Must pass handler of type ExtensionHandler to ext decorator")

        return f(*args, **kwargs)

    return the_wrapper_around_the_original_function