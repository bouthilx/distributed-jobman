import logging
import signal
import sys
import threading
import time

from jobman.api0 import DbHandle
from sqlalchemy.exc import TimeoutError
from sqlalchemy.orm.session import Session


logger = logging.getLogger("utils")


def bold(string, out=sys.stdout):
    if out.isatty():
        return '\x1b[1m%s\x1b[0m' % string
    else:
        return string


# http://code.activestate.com/recipes/577058/
def query_yes_no(question):
    """Ask a yes/no question via raw_input() and return their answer.

    "question" is a string that is presented to the user.

    The "answer" return value is True for "yes" or False for "no".
    """
    valid = {"yes": True, "y": True, "ye": True,
             "no": False, "n": False}
    prompt = " [y/n] "

    while True:
        sys.stdout.write(question + prompt)
        choice = raw_input().lower()
        if choice in valid:
            return valid[choice]
        else:
            sys.stdout.write("Please respond with 'yes' or 'no' "
                             "(or 'y' or 'n').\n")


def hash_dicts(iterable):
    hashed = []
    for item in iterable:
        if isinstance(item, dict):
            items = item.items()
            items.sort()
            hashed.append(hash_dicts(items))
        elif isinstance(item, (list, tuple)):
            hashed.append(hash_dicts(item))
        elif isinstance(item, DbHandle):
            hashed.append(item.tablename)
        elif isinstance(item, Session):
            hashed.append(None)
        else:
            hashed.append(item)

    return tuple(hashed)


class Cache(object):
    _caches = {}
    _timeouts = {}

    def __init__(self, timeout=2):
        self.timeout = timeout

    def collect(self):
        """Clear cache of results which have timed out"""
        for func in self._caches:
            cache = {}
            for key in self._caches[func]:
                if (time.time() - self._caches[func][key][1]) < self._timeouts[func]:
                    cache[key] = self._caches[func][key]
            self._caches[func] = cache

    def __call__(self, f):
        self.cache = self._caches[f.func_name] = {}
        self._timeouts[f.func_name] = self.timeout

        def func(*args, **kwargs):
            kw = kwargs.items()
            kw.sort()
            key = (hash_dicts(args), hash_dicts(tuple(kw)))
            try:
                v = self.cache[key]
                if (time.time() - v[1]) > self.timeout:
                    raise KeyError
            except KeyError:
                v = self.cache[key] = f(*args, **kwargs), time.time()
            return v[0]

        func.func_name = f.func_name

        return func


class Timer(object):

    def __init__(self, seconds, custom_timer_handler):
        self.seconds = seconds
        self.custom_timer_handler = custom_timer_handler

    def timer_handler(self, signal_number, frame):
        logger.debug("Timeout")
        self.custom_timer_handler()
        raise TimeoutError("transaction took more than %s seconds to execute" %
                           self.seconds)

    def __enter__(self):
        logger.debug("Set timer for %d seconds..." % self.seconds)

        self.t = threading.Timer(self.seconds, self.custom_timer_handler)
        self.t.start()

    def __exit__(self, exception_type, exception_val, traceback):
        logger.debug("Timer canceled")
        self.t.cancel()


class SafeSession(object):

    def __init__(self, db):
        self.db = db

    def set_timer(self, seconds):
        def custom_timer_handler():
            print "rollback"
            with Timer(5, lambda: sys.exit(1)):
                self.rollback()
        return Timer(seconds, custom_timer_handler)

    def handle_interrupt(self, signal_number, frame):
        self.restore_signal_handlers()
        #self.rollback()

    def rollback(self):
        logger.warning("An error occured during transaction, session is "
                       "rolled back.")
        self.session.rollback()
        self.session.close()

    def restore_signal_handlers(self):
        signal.signal(signal.SIGINT, self.previous_sigint_handler)
        signal.signal(signal.SIGTERM, self.previous_sigterm_handler)

    def __enter__(self):
        logger.debug("open safe session")
        self.session = self.db.session()
        self.previous_sigint_handler = signal.signal(
            signal.SIGINT, self.handle_interrupt)
        self.previous_sigterm_handler = signal.signal(
            signal.SIGTERM, self.handle_interrupt)
        return self

    def __exit__(self, exception_type, exception_val, traceback):
        if exception_type is not None:
            self.handle_interrupt(None, None)
            raise
        else:
            self.restore_signal_handlers()
            self.session.close()
            logger.debug("safe session closed")
            return True
