import sqlite3
import sys
import os

print(sys._getframe().f_lineno)  # 当前行数

# 打印 (comman + 鼠标左键) 可直接跳转到当前文件行
current_file_full_path = os.path.abspath(__file__)
# print(f"File \"{current_file_full_path}\", line {sys._getframe().f_lineno},")


class MigrateSqlite3DbHandle(object):
    """
    迁移 sqlite3 数据库 修改行: 196, 197 数据库路径
    """
    def query_all_tables_name(self, db_path):
        # 读取所有的表名称
        conn = sqlite3.connect(db_path)
        cursor1 = conn.cursor()
        sql_query_table_name = 'SELECT name FROM sqlite_master WHERE type=\'table\' ORDER BY name;'
        all_tables = cursor1.execute(sql_query_table_name)
        tables_list = []
        for t in all_tables:
            tables_list.append(t[0])
        # print(f"File \"{current_file_full_path}\", line {sys._getframe().f_lineno},", tables_list)
        conn.close()
        return tables_list

    def query_table_columns(self, table_name, db_path):
        # 查询表所有列
        sql_query_columns = f"PRAGMA table_info([{table_name}])"
        conn = sqlite3.connect(db_path)
        cursor1 = conn.cursor()
        all_columns = cursor1.execute(sql_query_columns)
        all_columns_list = []
        for c in all_columns:
            all_columns_list.append(c)
        # print(f"File \"{current_file_full_path}\", line {sys._getframe().f_lineno},", all_columns_list)
        conn.close()
        return all_columns_list

    def create_query_columns_str(self, old_columns_info_list, new_columns_info_list):
        # 列转换为查询或插入的字符串
        column_str = ''
        for column_obj in new_columns_info_list:
            current_column_name = column_obj[1]
            if self.judge_columns_info_list_exists_column(current_column_name, *old_columns_info_list):
                if column_str == '':
                    column_str = current_column_name
                else:
                    column_str = column_str + ', ' + current_column_name
        # if len(column_str) > 1:
        #     column_str = '(' + column_str + ')'
        # print(f"File \"{current_file_full_path}\", line {sys._getframe().f_lineno},", column_str)
        return column_str

    def create_insert_columns_str(self, columns_info_list):
        # 创建插入列 字符串
        column_str = ''
        for column_obj in columns_info_list:
            current_column_name = column_obj[1]
            if column_str == '':
                column_str = current_column_name
            else:
                column_str = column_str + ', ' + current_column_name
        if len(column_str) > 1:
            column_str = '(' + column_str + ')'
        return column_str

    def create_insert_default_column(self, query_columns_str, insert_columns_str):
        # 创建插入列 不存在 查询列 的列 list
        split_str = insert_columns_str.replace(query_columns_str, '', 1)
        split_str = split_str.strip('()')
        split_list = split_str.split(',')
        ret_list = []
        for c in split_list:
            if len(c) > 0:
                ret_list.append(c)
        return ret_list

    def query_column_type(self, column_name, new_columns_info_list):
        # 查询列数据类型
        for c_list in new_columns_info_list:
            if c_list[1] == column_name:
                return c_list[2]
        return None

    def single_insert_data_add_default_value(self, single_insert_data, new_columns_info_list, *default_columns):
        # 单条数据增加默认值
        column_types_default_value = self.column_types_default_value()
        for c in default_columns:
            c_type = self.query_column_type(c, new_columns_info_list)
            default_value = ''
            if c_type:
                default_value = column_types_default_value.get(c_type, '')
            single_insert_data.append(default_value)
        return single_insert_data
    
    def insert_data_add_default_value(self, query_columns_str, new_columns_info_list, *insert_data):
        # 插入数据添加默认值
        insert_columns_str = self.create_insert_columns_str(new_columns_info_list)
        default_columns = self.create_insert_default_column(query_columns_str, insert_columns_str)
        if not default_columns:
            # 查询列和插入列相同，不需要添加默认值
            return insert_data
        ret_data_list = []
        for single_data in insert_data:
            updated_single_data = self.single_insert_data_add_default_value(single_data, new_columns_info_list, *default_columns)
            ret_data_list.append(updated_single_data)
        return ret_data_list
        

    def judge_columns_info_list_exists_column(self, column_name, *columns_info_list):
        # 判断表列list 是否包含 指定列名称
        for column_info in columns_info_list:
            if column_info[1] == column_name:
                return True
        return False

    def column_types_default_value(self, column_type):
        default_value_dict = {
            'varchar': '',
            'bool': 1,
            'integer': 0,
            'datetime': '1970-01-01 00:00:00 UTC'
        }
        return default_value_dict

    def query_data(self, query_columns_str, table_name, db_path):
        # 查询数据
        sql = f"SELECT {query_columns_str} FROM {table_name};"
        # print(f"File \"{current_file_full_path}\", line {sys._getframe().f_lineno},", sql)
        conn = sqlite3.connect(db_path)
        cursor1 = conn.cursor()
        queryset = cursor1.execute(sql)
        q_data = []
        for item in queryset:
            q_data.append(item)
        # print(f"File \"{current_file_full_path}\", line {sys._getframe().f_lineno},", query_data)
        conn.close()
        return q_data

    def create_replace_str(self, insert_columns_str):
        # 创建占位符
        place_count = insert_columns_str.count(',')
        # print(f"File \"{current_file_full_path}\", line {sys._getframe().f_lineno},", place_count)
        replace_str = ''
        i = 0
        while i < place_count + 1:
            if replace_str == '':
                replace_str = '?'
            else:
                replace_str = replace_str + ', ' + '?'
            i = i + 1
        replace_str = '(' + replace_str + ')'
        return replace_str

    def insert_data(self, insert_columns_str, table_name, db_path, *data_list):
        # 插入数据
        conn = sqlite3.connect(db_path)
        cursor1 = conn.cursor()
        replace_str = self.create_replace_str(insert_columns_str)
        sql = f"INSERT INTO {table_name} {insert_columns_str} VALUES {replace_str};"
        # print(f"File \"{current_file_full_path}\", line {sys._getframe().f_lineno},", insert_columns_str, ';', replace_str, sql)
        try:
            for single_data in data_list:
                cursor1.execute(sql, single_data)
            conn.commit()
        except Exception as e:
            print(f"File \"{current_file_full_path}\", line {sys._getframe().f_lineno},", e)
            return False
        cursor1.close()
        conn.close()
        return True

    def query_data_and_insert_to_new_db(self, table_name, old_db_path, new_db_path):
        # 查询并插入数据
        old_columns_info_list = self.query_table_columns(table_name, old_db_path)
        new_columns_info_list = self.query_table_columns(table_name, new_db_path)
        # print(f"File \"{current_file_full_path}\", line {sys._getframe().f_lineno},", old_columns_info_list, new_columns_info_list)
        query_columns_str = self.create_query_columns_str(old_columns_info_list, new_columns_info_list)
        # print(f"File \"{current_file_full_path}\", line {sys._getframe().f_lineno},", query_columns_str)
        # 查询数据
        if len(query_columns_str) > 1 and len(table_name) > 0:
            queried_data_list = self.query_data(query_columns_str, table_name, old_db_path)
            # 数据插入默认值
            queried_data_list = self.insert_data_add_default_value(query_columns_str, new_columns_info_list, *queried_data_list)
            # 创建插入列 字符串
            insert_columns_str = self.create_insert_columns_str(new_columns_info_list)
            # 返回插入数据结果
            return self.insert_data(insert_columns_str, table_name, new_db_path, *queried_data_list)
        return None

if __name__ == '__main__':
    # ~~~~~~ 修改 sqlite3 路径 ~~~~~~
    old_db_path = '/Users/zhaoshenghua/development/programs/customer_dropter_purchase_record/server/100/customer_dropter_purchase_record_server/db_copy1.sqlite3'
    new_db_path = '/Users/zhaoshenghua/development/programs/customer_dropter_purchase_record/server/100/customer_dropter_purchase_record_server/db.sqlite3'
    # ~~~~~~ 修改 sqlite3 路径 ~~~~~~

    migrate_sqlite3_db_handle_class = MigrateSqlite3DbHandle()

    old_db_tables = migrate_sqlite3_db_handle_class.query_all_tables_name(old_db_path)
    new_db_tables = migrate_sqlite3_db_handle_class.query_all_tables_name(new_db_path)
    error_migration_tables = []
    for new_table in new_db_tables:
        # print(new_table)
        if new_table in old_db_tables and len(new_table) > 0:
            insert_data_result = migrate_sqlite3_db_handle_class.query_data_and_insert_to_new_db(new_table, old_db_path, new_db_path)
            if not insert_data_result:
                error_migration_tables.append(new_table)
    print(f"File \"{current_file_full_path}\", line {sys._getframe().f_lineno}, 数据迁移失败表:", error_migration_tables)

