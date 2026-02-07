import os
import sys
import logging
import threading
from hashlib import sha256
from urllib.parse import urlparse


class BatchStreamHandler(logging.Handler):
    """Buffers log messages per logger name, flushing as a block."""
    _write_lock = threading.Lock()

    def __init__(self, batch_size=5):
        super().__init__()
        self.batch_size = batch_size
        self._buffers = {}
        self._buf_lock = threading.Lock()

    def emit(self, record):
        msg = self.format(record)
        key = getattr(record, 'domain', record.name)
        flush_lines = None
        with self._buf_lock:
            buf = self._buffers.setdefault(key, [])
            buf.append(msg)
            if len(buf) >= self.batch_size:
                flush_lines = list(buf)
                buf.clear()
        if flush_lines is not None:
            self._write_block(flush_lines)

    def flush(self):
        """Flush all buffered messages (called on shutdown)."""
        with self._buf_lock:
            all_buffers = {k: list(v) for k, v in self._buffers.items() if v}
            self._buffers.clear()
        for name in sorted(all_buffers):
            self._write_block(all_buffers[name])

    def close(self):
        self.flush()
        super().close()

    def _write_block(self, lines):
        block = "\n".join(lines) + "\n\n"
        with BatchStreamHandler._write_lock:
            sys.stderr.write(block)
            sys.stderr.flush()


class _TruncateStatusFilter(logging.Filter):
    def filter(self, record):
        record.msg = record.msg.split(", status")[0]
        return True

_batch_handler = None

def _get_batch_handler():
    global _batch_handler
    if _batch_handler is None:
        _batch_handler = BatchStreamHandler(batch_size=50)
        _batch_handler.setLevel(logging.INFO)
        formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        _batch_handler.setFormatter(formatter)
        _batch_handler.addFilter(_TruncateStatusFilter())
    return _batch_handler

def get_logger(name, filename=None):
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    if not os.path.exists("Logs"):
        os.makedirs("Logs")
    fh = logging.FileHandler(f"Logs/{filename if filename else name}.log")
    fh.setLevel(logging.DEBUG)
    formatter = logging.Formatter(
       "%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    fh.setFormatter(formatter)
    logger.addHandler(fh)
    if filename == "Worker":
        logger.addHandler(_get_batch_handler())
    else:
        ch = logging.StreamHandler()
        ch.setLevel(logging.INFO)
        ch.setFormatter(formatter)
        logger.addHandler(ch)
    return logger


def get_urlhash(url):
    parsed = urlparse(url)
    # everything other than scheme.
    return sha256(
        f"{parsed.netloc}/{parsed.path}/{parsed.params}/"
        f"{parsed.query}/{parsed.fragment}".encode("utf-8")).hexdigest()

def normalize(url):
    if url.endswith("/"):
        return url.rstrip("/")
    return url