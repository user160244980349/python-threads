import functools
import uuid
from datetime import datetime
from multiprocessing.pool import ThreadPool
from multiprocessing import Pool, Process
from threading import Thread

from mysql.connector import connect


thread_count = 12

config = {
  "user": "user1",
  "password": "secret",
  "host": "localhost",
  "database": "test_database"
}

query = "INSERT INTO test_table " \
        "(f{}, f{}, f{}, f{}, f{}, f{}, f{}) " \
        "VALUES " \
        "('{}', '{}', '{}', '{}', '{}', '{}', '{}');"


def timeit(msg):
    def decorator_print(func):
        @functools.wraps(func)
        def wrapper_print(*args, **kwargs):
            start = datetime.now()
            func(*args, **kwargs)
            print(f"{msg} {datetime.now() - start}")
        return wrapper_print
    return decorator_print


def split_data(data, chunks):
    ld = len(data)
    return [[data[i] for i in range(ch, ld, chunks)] for ch in range(chunks)]


def job(data):
    cn = connect(**config)
    try:
        for i in data:
            with cn.cursor() as cr:
                cr.execute(query.format(*[j for j in range(1, 8)],
                                        *[i for _ in range(1, 8)]))
                # cn.commit()
            # cn.commit()
        cn.commit()
    finally:
        cn.close()


class WorkerThread(Thread):
    def __init__(self, data):
        super().__init__()
        self.data = data

    def run(self):
        job(self.data)


class WorkerProcess(Process):
    def __init__(self, data):
        super().__init__()
        self.data = data

    def run(self):
        job(self.data)


@timeit(f"threads time:")
def threads(data):
    # ths = [WorkerThread(d) for d in data]
    ths = [Thread(target=job, args=(d,)) for d in data]

    for t in ths:
        t.start()

    for t in ths:
        t.join()


@timeit(f"processes time:")
def processes(data):
    # procs = [WorkerProcess(d) for d in data]
    procs = [Process(target=job, args=(d,)) for d in data]

    for p in procs:
        p.start()

    for p in procs:
        p.join()


@timeit(f"threadpool time:")
def threadpool(data):
    with ThreadPool(thread_count) as p:
        p.map(job, data, chunksize=1)


@timeit(f"processpool time:")
def processpool(data):
    with Pool(thread_count) as p:
        p.map(job, data, chunksize=1)


@timeit(f"plain time:")
def plain(data):
    job(data)


def fetchall():
    with connect(**config) as cn:
        try:
            with cn.cursor() as cr:
                cr.execute("SELECT * FROM test_table")
                return cr.fetchall()
        finally:
            cn.close()


# sudo docker rm -f $(sudo  docker ps -a -q)
# sudo docker volume rm $(sudo docker volume ls -q)
# sudo docker-compose up --build
# https://github.com/user160244980349/python-threads
# https://ru.stackoverflow.com/questions/1175448/%D0%94%D0%BB%D1%8F-%D1%87%D0%B5%D0%B3%D0%BE-%D0%BD%D1%83%D0%B6%D0%B5%D0%BD-commit-connection-cursor-%D0%B8-close
# https://stackoverflow.com/questions/27912048/how-to-handle-mysql-connections-with-python-multithreading
def main():

    data = [uuid.uuid4() for _ in range(10000)]
    split = split_data(data, thread_count)

    plain(data)
    print(len(fetchall()))

    threads(split)
    print(len(fetchall()))

    threadpool(split)
    print(len(fetchall()))

    processes(split)
    print(len(fetchall()))

    processpool(split)
    print(len(fetchall()))


if __name__ == '__main__':
    main()
