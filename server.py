import sys
import base64
import os.path

import flask

from two1.wallet import Wallet
from two1.bitserv.flask import Payment


app = flask.Flask(__name__)
wallet = Wallet()
payment = Payment(app, wallet)


READ_SATOSHIS_PER_BYTE = 0.02
WRITE_SATOSHIS_PER_BYTE = 0.05


# Price a read request
def price_read(request):
    if request.args.get('op') == 'read':
        size = int(request.args['size'])
        return max(int(size * READ_SATOSHIS_PER_BYTE), 1)
    return 0


# Price a write request
def price_write(request):
    payload = flask.request.json
    if 'data' in payload:
        size = len(base64.b64decode(payload['data']))
        return max(int(size * WRITE_SATOSHIS_PER_BYTE), 1)
    return 0

def get_size(start_path = '.'):
    total_size = 0
    files = 0
    directories = 0
    for dirpath, dirnames, filenames in os.walk(start_path):
        directories += 1
        for f in filenames:
            fp = os.path.join(dirpath, f)
            total_size += os.path.getsize(fp)
            files += 1
    return total_size, files, directories

@app.route('/')
def index():
    path = app.config['serve_dir']
    size, files, directories = get_size(path)

    return flask.render_template('index.html', size=size,
                                 files=files,
                                 directories=directories,
                                 read_price=READ_SATOSHIS_PER_BYTE,
                                 write_price=WRITE_SATOSHIS_PER_BYTE)


@app.route("/files/<path:path>", methods=['HEAD'])
@app.route('/files/', defaults={'path': '/'}, methods=['HEAD'])
def files_head(path):
    """
    access(path) -> HEAD /files/<path>
        200
        404     Not Found
    """
    if path == "/":
        path = app.config['serve_dir']
    else:
        path = flask.safe_join(app.config['serve_dir'], path)

    if not os.path.exists(path):
        return "File not found.", 404

    return ""


@app.route("/files/<path:path>", methods=['GET'])
@app.route('/files/', defaults={'path': '/'}, methods=['GET'])
@payment.required(price_read)
def files_get(path):
    """
    getattr(path) -> GET /files/<path>?op=getattr
        200     {"st_mode": <mode int>, "st_size": <size int>}
        404     Not Found

    readdir(path) -> GET /files/<path>?op=readdir
        200     {"files": ["<file str>", ... ]}
        404     Not Found

    read(path, size, offset) -> GET /files/<path>?op=read&size=<size>&offset=<offset>
        200     "<base64 str>"
        404     Not Found
    """
    if 'op' not in flask.request.args:
        return 'Missing operation.', 400

    op = flask.request.args['op']

    if path == "/":
        path = app.config['serve_dir']
    else:
        path = flask.safe_join(app.config['serve_dir'], path)

    if op == 'getattr':
        try:
            info = os.stat(path)
            return flask.jsonify({'st_mode': info.st_mode, 'st_size': info.st_size})
        except FileNotFoundError:
            return 'File not found.', 404
    elif op == 'readdir':
        try:
            return flask.jsonify({"files": os.listdir(path)})
        except FileNotFoundError:
            return 'File not found.', 404
    elif op == 'read':
        if 'size' not in flask.request.args:
            return 'Missing size.', 400
        elif 'offset' not in flask.request.args:
            return 'Missing offset.', 400

        size = int(flask.request.args['size'])
        offset = int(flask.request.args['offset'])

        # Open, seek, read, close
        try:
            fd = os.open(path, os.O_RDONLY)
            os.lseek(fd, offset, os.SEEK_SET)
            buf = os.read(fd, size)
            os.close(fd)
        except FileNotFoundError:
            return 'File not found.', 404

        return flask.jsonify({"data": base64.b64encode(buf).decode()})

    return 'Unknown operation.', 400


@app.route("/files/<path:path>", methods=['POST'])
def files_post(path):
    """
    create(path) -> POST /files/<path>?op=create
        200

    mkdir(path) -> POST /files/<path>?op=mkdir
        200
        400     Directory exists.
    """
    if 'op' not in flask.request.args:
        return 'Missing operation.', 400

    op = flask.request.args['op']
    path = flask.safe_join(app.config['serve_dir'], path)

    if op == "create":
        fd = os.open(path, os.O_WRONLY | os.O_CREAT, 0o755)
        os.close(fd)
        return ""
    elif op == "mkdir":
        try:
            fd = os.mkdir(path, 0o755)
        except FileExistsError:
            return 'Directory exists.', 400
        return ""

    return 'Unknown operation.', 400


@app.route("/files/<path:path>", methods=['PUT'])
@payment.required(price_write)
def files_put(path):
    """
    write(path, data, offset) -> PUT /files/<path>
        {"data": "<base64 str>", "offset": <offset int>}

        200     <bytes written int>
        404     File not found.
    """
    path = flask.safe_join(app.config['serve_dir'], path)

    payload = flask.request.json

    if 'data' not in payload:
        return 'Missing data.', 400
    elif 'offset' not in payload:
        return 'Missing offset.', 400

    data = base64.b64decode(payload['data'])
    offset = int(payload['offset'])

    # Open, seek, write, close
    try:
        fd = os.open(path, os.O_WRONLY)
        os.lseek(fd, offset, os.SEEK_SET)
        n = os.write(fd, data)
        os.close(fd)
    except FileNotFoundError:
        return 'File not found.', 404

    return flask.jsonify({"count": n})


@app.route("/files/<path:path>", methods=['DELETE'])
def files_delete(path):
    """
    unlink(path) -> DELETE /files/<path>?op=unlink
        200
        404     File not found.

    rmdir(path) -> DELETE /files/<path>?op=rmdir
        200
        404     File not found.
    """
    if 'op' not in flask.request.args:
        return 'Missing operation.', 400

    op = flask.request.args['op']
    path = flask.safe_join(app.config['serve_dir'], path)

    if op == 'unlink':
        try:
            os.unlink(path)
        except FileNotFoundError:
            return 'File not found.', 404
        return ""
    elif op == 'rmdir':
        try:
            os.rmdir(path)
        except FileNotFoundError:
            return 'File not found.', 404
        except OSError as e:
            return 'Errno {}'.format(e.errno), 400
        return ""

    return 'Unknown operation.', 400


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: {} <serve directory>".format(sys.argv[0]))
        sys.exit(1)

    app.config['serve_dir'] = sys.argv[1]
    app.run(host="0.0.0.0", port=9123, debug=True)