#!/usr/bin/python
# -*- coding: utf-8 -*-

# python 3 环境
# 激活 activate.sh (激活py35 env，启动运行程序

import sys
import time
from datetime import datetime
# import commands
import os
import subprocess
import psutil

# 将repostory的目录i，作为根目录，添加到系统环境中。
ROOT_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
sys.path.append(ROOT_PATH)

from vnpy.trader.util_wechat import send_wx_msg

# python容器文件
python_path = '/home/trade/anaconda3/envs/py37/bin/python'

# shell 文件，不使用sh
bash = "/bin/bash"

# 配置内容
# 运行时间段
# 是否7X24小时运行（数字货币）
IS_7x24 = False
#  是否激活夜盘
ACTIVE_NIGHT = False
# python 脚本，这里要和activate.sh里面得PROGRAM_NAME 一致
PROGRAM_NAME = './run_service.py'

# 日志目录
log_path = os.path.abspath(os.path.join(os.getcwd(), 'log'))
if os.path.isdir(log_path):
    # 如果工作目录下，存在logs子目录，就使用logs子目录
    base_path = os.getcwd()
else:
    # 使用service.py所在得目录
    base_path = os.path.abspath(os.path.dirname(os.path.realpath(__file__)))

# 进程组id保存文件
gpid_file = os.path.abspath(os.path.join(base_path, 'log', 'gpid.txt'))

tmp_cron_file = os.path.abspath(os.path.join(base_path, 'log', 'cron.tmp'))

program_file = os.path.join(base_path, 'activate.sh')
log_file = os.path.abspath(os.path.join(base_path, 'log', 'service.log'))
error_file = os.path.abspath(os.path.join(base_path, 'log', 'service-error.log'))
cron_log_file = os.path.abspath(os.path.join(base_path, 'log', 'cron.log'))
cron_error_file = os.path.abspath(os.path.join(base_path, 'log', 'cron-error.log'))
null_file = "/dev/null"

cron_content = "* * * * * {} {} schedule >>{} 2>>{}".format(python_path, os.path.realpath(__file__), cron_log_file,
                                                            cron_error_file)
program_command = "nohup {} {} >>{} 2>>{} &".format(bash, program_file, log_file, error_file)

USE_GPID = False


def _check_gpid(gpid):
    """
    检查进程(组)ID
    :param gpid:
    :return: True, 正在运行/ False: 没有运行
    """
    if not USE_GPID:
        int_gpid = int(gpid)
        return psutil.pid_exists(int_gpid)

    try:
        # 通过系统子进程，打开ps命令，找到gpid下得所有进程
        p = subprocess.Popen(["ps", "-A", "-o", "pgrp="], stdin=subprocess.PIPE,
                             stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=False)
        returncode = p.wait()
    except OSError as e:
        print('找不到shell运行命令ps', file=sys.stderr)
        exit(1)

    # print('returncode1:{}'.format(returncode))
    try:
        p2 = subprocess.Popen("uniq", stdin=p.stdout, stdout=subprocess.PIPE,
                              stderr=subprocess.PIPE, shell=False)
        returncode = p2.wait()
    except OSError as e:
        print(u'找不到shell运行命令uniq', file=sys.stderr)
        exit(1)

    # print('returncode2:{}'.format(returncode))
    for i in p2.stdout.readlines():
        # print (u'p2.line:{}'.format(i))
        if i.decode().strip() == gpid:
            print(u'找到gpid:{}'.format(gpid))
            return True

    print(u'找不到gpid:{}'.format(gpid))
    return False


def _status():
    """
    查询当前状态
    :return:
    """
    print(u'检查{}'.format(gpid_file))
    if os.path.exists(gpid_file):
        with open(gpid_file, 'r') as f:
            gpid = f.read().strip()
            print(u'gpid={}'.format(gpid))
        if gpid != "" and _check_gpid(gpid):
            return gpid

    return None


def trade_off():
    """检查现在是否为非交易时间"""
    now = datetime.now()

    # 数字货币
    if IS_7x24:
        if now.hour == 12 and now.minute == 0:
            return True
        else:
            return False

    # 国内期货/股票
    a = datetime.now().replace(hour=2, minute=35, second=0, microsecond=0)
    b = datetime.now().replace(hour=9, minute=0, second=0, microsecond=0)
    c = datetime.now().replace(hour=15, minute=30, second=0, microsecond=0)
    d = datetime.now().replace(hour=20, minute=45, second=0, microsecond=0)

    # 国内期货有夜盘
    if ACTIVE_NIGHT:
        weekend = (now.isoweekday() == 6 and now >= a) or (now.isoweekday() == 7) or (
                now.isoweekday() == 1 and now <= a)
        off = (a <= now <= b) or (c <= now <= d) or weekend
        return off
    else:
        weekend = now.isoweekday() in [6, 7]
        off = now <= b or c <= now or weekend
        return off


def _start():
    """
    启动服务
    :return:
    """
    # 获取进程组id
    gpid = _status()
    if trade_off():
        # 属于停止运行期间
        if gpid:
            print(u'现在属于停止运行时间，进程组ID存在,将杀死服务进程:[gpid={}]'.format(gpid))
            import signal
            if USE_GPID:
                # 杀死进程组
                os.killpg(int(gpid), signal.SIGKILL)
            else:
                os.kill(int(gpid), signal.SIGKILL)
            i = 0
            while _status():
                time.sleep(1)
                i += 1
                print(u'杀死进程中，等待{}秒'.format(i))
                if i > 30:
                    print(u'杀死进程失败，退出')
                    exit(1)

            print('进程组已停止运行[gpid={}]'.format(gpid))
            send_wx_msg('进程组{}已停止运行[{}]'.format(gpid, base_path))
        else:
            print(u'{} 现在属于停止运行时间，不启动服务'.format(datetime.now()))
    else:
        # 属于运行时间
        if not gpid:
            print(u'{}属于运行时间,将启动服务:{}'.format(datetime.now(), program_command))
            if os.path.isfile(gpid_file):
                print(u'{0}文件存在，先执行删除'.format(gpid_file))
            try:
                os.remove(gpid_file)
            except:
                pass

            os.popen(program_command)
            i = 0
            while True:
                gpid = _status()
                if gpid:
                    print('{}属于运行时间,成功启动服务[gpid={}]'.format(datetime.now(), gpid))
                    send_wx_msg('{}属于运行时间,成功启动服务[{},gpid={}]'.format(datetime.now(), base_path, gpid))
                    break

                i += 1
                print(u'启动进程中，等待{}秒'.format(i))
                if i > 30:
                    print(u'启动进程失败，退出')
                    exit(1)
                time.sleep(1)


        else:
            print(u'{}属于运行时间,{}服务已运行'.format(datetime.now(), base_path))


def schedule():
    """
    crontab 计划执行
    :return:
    """
    print('======schedule========')
    _start()


def status():
    """查看状态"""
    print('======status========')

    gpid = _status()
    if gpid:
        print('{}服务进程[gpid={}]正在运行'.format(base_path, gpid))
    else:
        print('{}服务进程没有运行.'.format(base_path))

    check_pids_in_cwd(gpid)


# operate的可选字符串为：add, del
def operate_crontab(operate):
    """
    操作crontab
    :param operate: add , del
    :return:
    """

    try:
        # 从系统命令中，获取定时任务
        p = subprocess.Popen(["crontab", "-l"], stdin=subprocess.PIPE,
                             stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=False)
        returncode = p.wait()
    except OSError as e:
        print(u"找不到shell运行命令crontab", file=sys.stderr)
        exit(1)

    remain_cron_list = []
    exist_flag = False
    old_cron_content = ''
    for i in p.stdout.readlines():
        if i.decode("utf-8").find(os.path.realpath(__file__) + " schedule") >= 0:
            old_cron_content = i.decode("utf-8")
            exist_flag = True
        else:
            remain_cron_list.append(i.decode("utf-8"))

    if operate == "add" and not exist_flag:
        remain_cron_list.append(cron_content)
        remain_cron_list.append("\n")
        with open(tmp_cron_file, 'wb') as f:
            for i in remain_cron_list:
                f.write(i.encode("utf-8"))
        os.popen("crontab {}".format(tmp_cron_file))
        print(u'添加crontab项: {}'.format(cron_content), file=sys.stderr)

    if operate == "del" and exist_flag:
        with open(tmp_cron_file, 'wb') as f:
            for i in remain_cron_list:
                f.write(i.encode("utf-8"))
        os.popen("crontab {}".format(tmp_cron_file))
        print(u'删除crontab item: {}'.format(old_cron_content), file=sys.stderr)

        # os.remove(tmp_cron_file)


def check_pids_in_cwd(gpid=None):
    print('检查{}路径下运行得python {}进程'.format(base_path, PROGRAM_NAME))

    runing_pids = []
    for pid in psutil.pids():
        try:
            p = psutil.Process(pid)
            p_name = p.name()
            if not p_name.endswith('python'):
                continue
            p_cwd = p.cwd()
            if p_cwd != base_path:
                continue
            p_cmdline = p.cmdline()
            if PROGRAM_NAME not in p_cmdline:
                continue

            runing_pids.append(pid)

        except:
            pass

    if len(runing_pids) > 1:
        if gpid is not None:
            if gpid in runing_pids:
                print(u'排除其他pid')
                runing_pids.remove(gpid)
            else:
                print(u'gpid，不在运行清单中，排除首个pid')
                runing_pids.pop(0)
        else:
            print(u'gpid为空，排除首个pid')
            runing_pids.pop(0)

        for pid in runing_pids:
            try:
                p = psutil.Process(pid)
                print(u'pid:{},name:{},bin:{},path:{},cmd:{}，被终止运行'
                      .format(pid, p.name, p.exe(), p.cwd(), p.cmdline()))
                import signal
                os.kill(int(pid), signal.SIGKILL)
            except:
                pass


def start():
    print(u'======start========')
    # 往任务表增加定时计划
    operate_crontab("add")
    print(u'任务表增加定时计划完毕')
    # 执行启动
    # _start()
    print(u'启动{}服务执行完毕'.format(base_path))


def _stop():
    print(u'======stop========')
    # 在任务表删除定时计划
    operate_crontab("del")

    # 查询进程组id
    gpid = _status()
    if gpid:
        # 进程组存在，杀死进程
        import signal
        # 杀死进程组
        if USE_GPID:
            # 杀死进程组
            os.killpg(int(gpid), signal.SIGKILL)
        else:
            os.kill(int(gpid), signal.SIGKILL)
        i = 0
        while _status():
            time.sleep(1)
            i += 1
            print(u'等待{}秒'.format(i))

        print(u'{}成功停止{}服务[gpid={}]'.format(datetime.now(), base_path, gpid))
        send_wx_msg(u'{}成功停止{}服务[gpid={}]'.format(datetime.now(), base_path, gpid))
    else:
        print(u'{}服务进程没有运行'.format(base_path))


def stop():
    """
    停止服务
    :return:
    """
    _stop()
    print(u'执行停止{}服务完成'.format(base_path))


def restart():
    print(u'======restart========')
    _stop()
    _start()
    print('执行重启{}服务完成'.format(base_path))


if __name__ == '__main__':
    if len(sys.argv) >= 2:
        fun = sys.argv[1]
    else:
        fun = ''
    if fun == 'status':
        status()
    elif fun == 'start':
        start()
    elif fun == 'stop':
        stop()
    elif fun == 'restart':
        restart()
    elif fun == 'schedule':
        schedule()
    else:
        print(u'Usage: {} (status|start|stop|restart)'.format(os.path.basename(__file__)))
        status()
