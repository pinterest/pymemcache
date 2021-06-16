import functools
import inspect
import time
import logging

import pymemcache.exceptions as pymemexception

logger = logging.getLogger(__name__)

default_strategies = {
    'global'
    'local'
}


class _retry_always(retry_base):
    """Retry strategy that always rejects any result."""

    def __call__(self, retry_state):
        return True


retry_always = _retry_always()


class retry_if_exception(retry_base):
    """Retry strategy that retries if an exception verifies a predicate."""

    def __init__(self, predicate):
        self.predicate = predicate

    def __call__(self, retry_state):
        if retry_state.outcome.failed:
            return self.predicate(retry_state.outcome.exception())
        else:
            return False


class retry_if_exception_type(retry_if_exception):
    """Retries if an exception has been raised of one or more types."""

    def __init__(self, exception_types=Exception):
        self.exception_types = exception_types
        super(retry_if_exception_type, self).__init__(
            lambda e: isinstance(e, exception_types)
        )


class retry_if_not_exception_type(retry_if_exception):
    """Retries except an exception has been raised of one or more types."""

    def __init__(self, exception_types=Exception):
        self.exception_types = exception_types
        super(retry_if_not_exception_type, self).__init__(
            lambda e: not isinstance(e, exception_types)
        )


class retry_unless_exception_type(retry_if_exception):
    """Retries until an exception is raised of one or more types."""

    def __init__(self, exception_types=Exception):
        self.exception_types = exception_types
        super(retry_unless_exception_type, self).__init__(
            lambda e: not isinstance(e, exception_types)
        )

    def __call__(self, retry_state):
        # always retry if no exception was raised
        if not retry_state.outcome.failed:
            return True
        return self.predicate(retry_state.outcome.exception())


class stop_any(stop_base):
    """Stop if any of the stop condition is valid."""

    def __init__(self, *stops):
        self.stops = stops

    def __call__(self, retry_state):
        return any(x(retry_state) for x in self.stops)


class stop_all(stop_base):
    """Stop if all the stop conditions are valid."""

    def __init__(self, *stops):
        self.stops = stops

    def __call__(self, retry_state):
        return all(x(retry_state) for x in self.stops)


class stop_never(stop_base):
    """Never stop."""

    def __call__(self, retry_state):
        return False


def retry(
    func,
    fretry,
    stop,
    wait,
    reraise,
    before,
    after,
    callback
):
    @functools.wraps(func)
    def func_call(*args, **kwargs):
        while True:
            try:
                return func(*args, **kwargs)
            except Exception as e:
                fretry()
                print(e)
                print(f"Error {ntries}")
            finally:
                ntries -= 1
                time.sleep(2)
    return func_call


class RetryingClient(object):
    """
    A client for communicating with a cluster of memcached servers

    strategy: template of retry strategy to use (default to 'global',
        all kind of exception will lead to retry)
    stop: Define when to stop retrying
    wait: Define how time to wait before two attempts (fixed, random, exponential backoff)
    reraise: Define if we should reraise the exceptions
    before: Define an action to execute before each attempts
    after: Define an action to execute after each attempts
    callback: Define a callback function to execute after all retries failed
    """

    def _decorate_client(self):
        """
        Morphing the class into the passed client to proxify all his methods
        call and then catch exceptions to start retrying over them.
        """
        for name, m in inspect.getmembers(self.client, inspect.ismethod):
            if name.startswith("_"):
                continue
            # Binding client's methods to our current object and decorate
            # them dynamically with our retry features.
            setattr(
                self,
                name,
                retry(
                    m,
                    self.retry
                    self.stop,
                    self.wait,
                    self.reraise,
                    self.before,
                    self.after,
                    self.callback
            ))

    def __init__(self,
            client,
            retry=retry_if_exception_type,
            stop=stop_never,
            wait,
            reraise,
            before,
            after,
            callback
        ):
        self.retry = retry
        self.stop = stop
        self.wait = wait
        self.reraise = reraise
        self.before = before
        self.after = after
        self.callback = callback
        self.client = client
        self._decorate_client()
