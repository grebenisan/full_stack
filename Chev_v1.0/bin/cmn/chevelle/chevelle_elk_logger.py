import sys
import os
import json
from flask import Flask, request, jsonify
import logging

app = Flask(__name__)
port_ = int(os.getenv("PORT"))

logger = logging.getLogger('Data Classification')
app.logger.setLevel(logging.DEBUG)
handler = logging.StreamHandler(stream=sys.stdout)
app.logger.addHandler(handler)


@app.route('/logging/chevelle_dc/stdout', methods=['POST'])
def log_stdout():
    payload = request.data
    log_item = json.loads(payload.decode('utf-8'))
    app.logger.info("{content}\t{status}".format(status=log_item['status'], content=log_item['content']))
    return "logged"

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=port_, debug=True)





