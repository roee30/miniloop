import asyncio
import logging
import time
from dataclasses import dataclass, field
from socket import socket
from typing import (
    Any,
    Callable,
    Coroutine,
    Literal,
    ClassVar,
    TypeVar,
    Awaitable,
    Generic,
)

from select import select


def setup_logging(log_: logging.Logger):
    formatter = logging.Formatter(
        "{asctime} - {levelname} - {name}:{lineno}.{funcName}() - {message}", style="{"
    )
    handler = logging.StreamHandler()
    handler.setFormatter(formatter)
    log_.addHandler(handler)
    for o in log_, handler:
        o.setLevel(logging.DEBUG)


log = logging.getLogger("loop")
setup_logging(log)


@dataclass
class Result:
    """
    Wrap task results in Ok() for returned value
    or Exc() for raised exception
    """

    value: Any


class Ok(Result):
    pass


class Exc(Result):
    pass


Target = Literal["read", "write"]
T = TypeVar("T")


@dataclass
class Future(Awaitable[T]):
    """
    A callback that will produce a value once ready.

    what: anything that can be used with select()
    target: reading or writing
    callback: the callback that will produce the value when present
    result: the callback's result
    """

    what: Any
    target: Target
    callback: Callable[[], T]
    result: Result | None = None

    def __await__(self):
        log.debug("awaiting %s", self)
        # yield control to the event loop to check whether result is ready with select()
        yield self
        # yield should only return when the event loop has decided that the callback
        # is ready to produce a result and resumed the future's coroutine with .send()
        log.debug(f"returning future result {self.result=}")
        # An error can only be assigned in Task.step(). Here the values are always Ok()
        assert isinstance(self.result, Ok)
        # return the result
        return self.result.value

    def fileno(self):
        """
        Make the future selectable with select()
        """
        return self.what.fileno()


@dataclass
class Task(Generic[T]):
    """
    A place for holding values passed from futures to coroutines
    co: the entry point coroutine
    index: an arbitrary identifier (shorter than id())
    counter: global counter for setting self.index
    """

    co: Coroutine[Future, ..., T] = field()
    counter: ClassVar[int] = 0
    index: int = field(init=False)

    def __repr__(self):
        return f"Task<{self.co.__name__}#{self.index}>"

    def __post_init__(self):
        self.index = self.counter
        type(self).counter += 1
        log.debug("created %s", self)
        self.result: Result | None = None
        self.next_value = None
        self.future = None

    def fileno(self):
        """
        for select()
        """
        assert self.future, self
        return self.future.fileno()

    def send(self, value: Any):
        log.debug("sending %s", value)
        return self.co.send(value)

    @property
    def done(self):
        """
        None means that the task has not finished.
        The result of None is represented with Ok(None)
        """
        return self.result is not None

    def step(self):
        """
        An awaited value has become ready. Update self.next_value
        """
        try:
            result = Ok(self.future.callback())
        except Exception as ex:
            result = Exc(ex)
        self.next_value = self.future.result = result

    def throw(self, error: Exception):
        return self.co.throw(error)


class UniqueList(list[T]):
    """
    A list with no duplicates. Used to detect faulty event loop implementations
    that add a value to a list which is already present.
    """

    def append(self, value: T):
        if value in self:
            raise Exception(f"value {value} already in self {self}")
        super().append(value)


class Loop(asyncio.AbstractEventLoop):
    """
    A minimal, incomplete implementation of a Python event loop.
    Enough to run a basic client or server.
    Not all AbstractEventLoop methods are implemented.
    """

    def __init__(self):
        # tasks that do not currently wait on anything or that their awaited value is ready
        self.tasks: list[Task] = UniqueList()
        # tasks that wait to read something
        self.read: list[Task] = UniqueList()
        # tasks that wait to write something
        self.write: list[Task] = UniqueList()
        # the user's entry point or "main thread" supplied to Loop.run_until_complete()
        self.entry_point: Task | None = None

    def _run_once(self):
        """
        Advance a single task one step forward
        """
        log.debug("_run_once()")
        try:
            task = self.tasks.pop(0)
        except IndexError:
            log.debug("tasks empty")
            return
        value = task.next_value
        match value:
            # a future should be awaited for in .select()
            case Future(_what, target):
                self._handle_future(task, target, value)
            case _:
                ok, value = self._handle_value(task, value)
                if not ok:
                    return
                log.debug("received value %s", value)
                # if we're here, a future has produced a result which is
                # ready to be handled by the task. mark the task as ready for next iteration
                log.debug("adding to tasks: %s", task)
                self.tasks.append(task)
                log.debug("setting value %s", value)
                # store the value for the next iteration
                task.next_value = value

    def _handle_future(self, task: Task, target: Target, value: Any):
        if target == "read":
            log.debug("adding to read: %s", task)
            self.read.append(task)
        elif target == "write":
            log.debug("adding to write: %s", task)
            self.write.append(task)
        else:
            unreachable()
        log.debug(f"setting future of {task} to {value}")
        # enables selecting on task
        task.future = value

    def _handle_value(self, task: Task, value: Any):
        """
        Returns is_ok, value
        """
        try:
            match value:
                case None:
                    # coroutine has not started. start it
                    value = task.send(None)
                case Ok(ok_value):
                    # a future produced a result
                    log.debug(f"{ok_value=}")
                    value = task.send(ok_value)
                case Exc(error):
                    # a future produced an error
                    log.debug(f"{error=}")
                    # let the task handle the error
                    value = task.throw(error)
                case _:
                    unreachable()
            return True, value
        except StopIteration as ex:
            # task has finished and returned a value
            result = ex.value
            log.debug(f"{task=} terminated with {result=}")
            task.result = Ok(result)
            return False, None
        except Exception as exception:
            # task has thrown an error
            log.debug(f"setting task.result={exception!r}")
            task.result = Exc(exception)
            # if this is the entry point supplied by the user (the "main thread"), propagate the error
            if task is self.entry_point:
                log.debug("reraising exception")
                raise
            # else, this is a spawned "thread". just note the error has not been handled
            log.error(f"Task exception was never retrieved, {task=}, {exception=!r}")
            return False, None

    def select(self):
        """
        Wait until at least one of the waiting tasks' value is ready
        """
        log.debug("selecting %s, %s", self.read, self.write)
        if self.read or self.write:
            read_ready, write_ready, _ = select(self.read, self.write, [])
            log.debug("removing from .read, .write: %s, %s", read_ready, write_ready)
            # tasks that are returned by select are no longer waiting, remove them from the lists
            for to_remove, lst in [
                (read_ready, self.read),
                (write_ready, self.write),
            ]:
                for task in to_remove:
                    lst.remove(task)
            # mark all selected tasks as ready for advancement
            all_ready = read_ready + write_ready
            log.debug("adding to tasks: %s", all_ready)
            self.tasks.extend(all_ready)
            for task in all_ready:
                log.debug("stepping %s", task)
                # update the task's next_value
                task.step()
        else:
            # nothing to wait on, this should not happen
            log.debug("no read")
            # just in case, avoid endless spamming
            time.sleep(0.5)

    def create_task(
        self, co: Coroutine[Future, ..., T], _name: str | None = None
    ) -> Task[T]:
        """
        Put a coroutine in the event loop
        """
        log.debug("creating task for %s", co)
        task: Task[T] = Task(co)
        log.debug("adding to tasks: %s", task)
        self.tasks.append(task)
        return task

    def run_forever(self) -> None:
        while True:
            if self.tasks:
                self._run_once()
            else:
                self.select()

    def run_until_complete(self, co: Coroutine[Future, ..., T]) -> T:
        """
        Run a task and its futures until it's complete.
        A task may spawn additional tasks, which are only advanced until the main task is done
        """
        # mark the task as the entry point
        task: Task[T] = self.create_task(co)
        self.entry_point = task
        log.debug("adding to tasks: %s", task)
        while not task.done:
            # select() blocks. so don't block if there's any task ready for advancing
            if self.tasks:
                self._run_once()
            else:
                # no ready tasks, wait for futures
                self.select()
        # task result should have been set by now
        assert task.result is not None
        # an error would have been propagated, therefore we have an Ok() value
        assert isinstance(task.result, Ok)
        # return the inner value
        return task.result.value

    def sock_accept(self, server: socket) -> Future[tuple[socket, tuple[str, int]]]:
        """
        Accept a new client from a listening server socket asynchronously
        """
        return Future(server, "read", server.accept)

    def sock_recv(self, socket_: socket, size: int) -> Future[bytes]:
        """
        Receive data from socket asynchronously
        """
        return Future(socket_, "read", lambda: socket_.recv(size))

    def sock_sendall(self, socket_: socket, payload: bytes) -> Future[None]:
        """
        Send data asynchronously.
        Note that this is "cheating" because socket.sendall() can block for large payloads,
        which will block the entire event loop
        """
        return Future(socket_, "write", lambda: socket_.sendall(payload))


loop = Loop()


def run(co: Coroutine[Future, ..., T]) -> T:
    return loop.run_until_complete(co)


def get_event_loop():
    return loop


def unreachable():
    assert False, "unreachable"
