import json
from ti_util import json_util
from datetime import datetime

json_str = json.dumps(datetime.now(), default=json_util.default_ts_str)
print(json_str)

print(json.loads(json_str, object_hook=json_util.object_hook_ts_str))

json_str = json.dumps('123', default=json_util.default_ts_str)
print(json_str)

print(json.loads(json_str, object_hook=json_util.object_hook_ts_str))