import pymysql
import os
import subprocess
import time
from datetime import datetime
import shutil
import configparser
import tempfile
from threading import Thread
def create_directory(path):
    if not os.path.exists(path):
        os.makedirs(path)
    return path

def test_connection(host, port, user, password):
    try:
        connection = pymysql.connect(host=host, port=int(port), user=user, password=password)
        connection.close()
        return True
    except pymysql.MySQLError as e:
        print("连接失败：", e)
        return False

def backup_single_database(database_name, tmpfile_name, current_date, sql_folder, interval):
    if database_name not in ['information_schema', 'performance_schema', 'mysql', 'sys']:
        db_folder = create_directory(os.path.join(sql_folder, database_name))
        date_folder = create_directory(os.path.join(db_folder, current_date))
        filename = f"{database_name}_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.sql"
        dump_path = os.path.join(date_folder, filename)
        dump_command = f"mysqldump --defaults-file={tmpfile_name} --force --opt --no-tablespaces {database_name} > {dump_path}"
        result = os.system(dump_command)

        if result == 0 and os.path.exists(dump_path):
            shutil.make_archive(dump_path[:-4], 'zip', date_folder, filename)
            os.remove(dump_path)
            print(f"数据库 {database_name} 备份成功保存在 {current_date} 文件夹内。")
        else:
            print(f"数据库 {database_name} 备份失败。尝试重新备份在 {interval} 秒后。")


def backup_database(host, port, user, password, interval):
    while True:
        connection = pymysql.connect(host=host, port=int(port), user=user, password=password)
        cursor = connection.cursor()
        cursor.execute("SHOW DATABASES;")
        databases = cursor.fetchall()
        cursor.close()
        connection.close()

        current_date = datetime.now().strftime("%Y-%m-%d")
        sql_folder = create_directory("sql")

        with tempfile.NamedTemporaryFile(mode='w+', delete=False) as tmpfile:
            tmpfile.write(f"[client]\nuser={user}\npassword={password}\nhost={host}\nport={port}")
            tmpfile_name = tmpfile.name

        threads = []
        for database in databases:
            database_name = database[0]
            thread = Thread(target=backup_single_database, args=(database_name, tmpfile_name, current_date, sql_folder, interval))
            threads.append(thread)
            thread.start()

        for thread in threads:
            thread.join()

        os.unlink(tmpfile_name)
        print(f"下次备份在 {interval} 秒后。")
        time.sleep(interval)



def save_config(host, port, user, password, interval):
    config = configparser.ConfigParser()
    config['MySQL'] = {
        'host': host,
        'port': port,
        'user': user,
        'password': password,
        'interval': interval
    }
    with open('config.txt', 'w') as configfile:
        config.write(configfile)

def load_config():
    config = configparser.ConfigParser()
    if os.path.exists('config.txt'):
        config.read('config.txt')
        return config['MySQL']
    return None

def main():
    config = load_config()
    if config:
        host = config['host']
        port = config['port']
        user = config['user']
        password = config['password']
        interval = int(config['interval'])
        print("使用保存的配置信息...")
    else:
        host = input("请输入数据库地址: ")
        port = input("请输入数据库端口号: ")
        user = input("请输入数据库账号: ")
        password = input("请输入数据库密码: ")
        interval = int(input("请输入备份时间间隔（以秒为单位）: "))
        save_config(host, port, user, password, interval)

    print("测试连接到数据库...")
    if test_connection(host, port, user, password):
        print("连接成功。开始自动备份...")
        backup_database(host, port, user, password, interval)
    else:
        print("连接失败。请检查输入的信息是否正确。")

if __name__ == "__main__":
    main()
