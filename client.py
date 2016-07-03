import sys
import errno
import logging
import base64
import threading

from fuse import FUSE, FuseOSError, Operations

from two1.wallet import Wallet
from two1.bitrequests import ChannelRequests


wallet = Wallet()
requests = ChannelRequests(wallet)


logging.basicConfig(level=logging.DEBUG)


class BitFs(Operations):

    def __init__(self, url):
        self._url = url
        self._lock = threading.Lock()

    def access(self, path, mode):
        with self._lock:
            r = requests.head(self._url + path)
        if r.status_code == 404:
            raise FuseOSError(errno.ENOENT)
        elif r.status_code != 200:
            raise FuseOSError(errno.EIO)

        return 0

    def getattr(self, path, fh):
        with self._lock:
            r = requests.get(self._url + path, params={'op': 'getattr'})
        if r.status_code == 404:
            raise FuseOSError(errno.ENOENT)
        elif r.status_code != 200:
            raise FuseOSError(errno.EIO)

        return r.json()

    def readdir(self, path, fh):
        with self._lock:
            r = requests.get(self._url + path, params={'op': 'readdir'})
        if r.status_code == 404:
            raise FuseOSError(errno.ENOENT)
        elif r.status_code != 200:
            raise FuseOSError(errno.EIO)

        return r.json()['files']

    def read(self, path, size, offset, fh):
        with self._lock:
            r = requests.get(self._url + path, params={'op': 'read', 'size': size, 'offset': offset})
        if r.status_code == 404:
            raise FuseOSError(errno.ENOENT)
        elif r.status_code != 200:
            raise FuseOSError(errno.EIO)

        return base64.b64decode(r.json()['data'])

    def create(self, path, mode):
        with self._lock:
            r = requests.post(self._url + path, params={'op': 'create'})
        if r.status_code != 200:
            raise FuseOSError(errno.EIO)

        return 0

    def mkdir(self, path, mode=0o755):
        with self._lock:
            r = requests.post(self._url + path, params={'op': 'mkdir'})
        if r.status_code == 400:
            raise FuseOSError(errno.EEXIST)
        elif r.status_code != 200:
            raise FuseOSError(errno.EIO)

        return 0

    def write(self, path, data, offset, fh):
        with self._lock:
            r = requests.put(self._url + path, json={'data': base64.b64encode(data).decode(), 'offset': offset})
        if r.status_code == 404:
            raise FuseOSError(errno.ENOENT)
        elif r.status_code != 200:
            raise FuseOSError(errno.EIO)

        return r.json()['count']

    def unlink(self, path):
        with self._lock:
            r = requests.delete(self._url + path, params={'op': 'unlink'})
        if r.status_code == 404:
            raise FuseOSError(errno.ENOENT)
        elif r.status_code != 200:
            raise FuseOSError(errno.EIO)

    def rmdir(self, path):
        with self._lock:
            r = requests.delete(self._url + path, params={'op': 'rmdir'})
        if r.status_code == 404:
            raise FuseOSError(errno.ENOENT)
        elif r.status_code != 200:
            raise FuseOSError(errno.EIO)

    def truncate(self, path, length):
        pass


if __name__ == '__main__':
    if len(sys.argv) != 3:
        print('Usage: {} <bitfs url> <mountpoint>'.format(sys.argv[0]))
        sys.exit(1)

    fuse = FUSE(BitFs(sys.argv[1]), sys.argv[2], foreground=True)