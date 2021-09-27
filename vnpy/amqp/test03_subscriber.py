# encoding: UTF-8

from vnpy.amqp.consumer import subscriber


if __name__ == '__main__':

    from time import sleep
    c = subscriber(host='192.168.1.211',user='admin', password='admin', exchange='x_fanout_idx_tick')

    c.subscribe()

    while True:
        sleep(1)
