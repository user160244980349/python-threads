import functools
import uuid
import threading
from datetime import datetime
from multiprocessing.pool import ThreadPool
from multiprocessing import Pool
from mysql.connector import connect


thread_count = 12

config = {
  'user': 'user1',
  'password': 'secret',
  'host': 'localhost',
  'database': 'test_database'
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


class WorkerThread(threading.Thread):
    def __init__(self, data):
        threading.Thread.__init__(self)
        self.data = data

    def run(self):
        job(self.data)


@timeit(f"threaded time:")
def threaded(data):
    threads = [WorkerThread(d) for d in data]

    for t in threads:
        t.start()

    for t in threads:
        t.join()


@timeit(f"threadpooled time:")
def threadpooled(data):
    with ThreadPool(thread_count) as p:
        p.map(job, data, chunksize=1)


@timeit(f"multiproc time:")
def multiproc(data):
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
# https://ru.stackoverflow.com/questions/1175448/%D0%94%D0%BB%D1%8F-%D1%87%D0%B5%D0%B3%D0%BE-%D0%BD%D1%83%D0%B6%D0%B5%D0%BD-commit-connection-cursor-%D0%B8-close
# https://stackoverflow.com/questions/27912048/how-to-handle-mysql-connections-with-python-multithreading
def main():

    data = [uuid.uuid4() for _ in range(10000)]
    split = split_data(data, thread_count)

    plain(data)
    print(len(fetchall()))
    threaded(split)
    print(len(fetchall()))
    multiproc(split)
    print(len(fetchall()))
    threadpooled(split)
    print(len(fetchall()))


if __name__ == '__main__':
    main()
