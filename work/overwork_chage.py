import psycopg2
from datetime import datetime, timedelta
import pandas as pd
from config import DB_CONFIG
import sys

# 强制刷新输出缓冲区
def flush_print(*args, **kwargs):
    """带缓冲刷新的print函数"""
    print(*args, **kwargs)
    sys.stdout.flush()

def get_db_connection():
    """创建数据库连接"""
    return psycopg2.connect(**DB_CONFIG)

def get_overwork_records(conn):
    """获取加班记录"""
    cursor = conn.cursor()
    try:
        cursor.execute("""
            SELECT 姓名, 
                   split_part(开始时间, ' ', 1) as 加班日期,
                   开始时间, 
                   结束时间, 
                   加班时长小时, 
                   加班说明, 
                   申请状态, 
                   数据来源
            FROM overwork
            WHERE 申请状态 IN ('已同意', '审批通过')
            ORDER BY 姓名, 开始时间
        """)
        records = cursor.fetchall()
        return records
    finally:
        cursor.close()

def update_attendance_for_overtime(cursor, name, date, overtime_hours, source):
    """更新考勤记录中的加班信息"""
    try:
        # 获取该员工在该日期的考勤记录
        cursor.execute("SELECT * FROM attendance_result WHERE 姓名 = %s", (name,))
        employee_record = cursor.fetchone()
        
        if not employee_record:
            flush_print(f"❌ 未找到员工 {name} 的记录")
            return
        
        # 获取列名
        cursor.execute("SELECT column_name FROM information_schema.columns WHERE table_name = 'attendance_result' ORDER BY ordinal_position")
        columns = [col[0] for col in cursor.fetchall()]
        
        # 创建列名到索引的映射
        col_index_map = {col: i for i, col in enumerate(columns)}
        
        # 构建更新语句
        day_column = f"第{date.day}天"
        if day_column in col_index_map:
            # 获取当前值
            current_value = employee_record[col_index_map[day_column]]
            
            # 构建新的值
            if current_value and str(current_value) != 'nan':
                new_value = f"{current_value} + {source}加班({overtime_hours}h)"
            else:
                new_value = f"{source}加班({overtime_hours}h)"
            
            # 更新记录
            update_query = f"UPDATE attendance_result SET \"{day_column}\" = %s WHERE 姓名 = %s"
            cursor.execute(update_query, (new_value, name))
            flush_print(f"✅ 已更新 {name} 在 {date.date()} 的考勤记录：{new_value}")
        else:
            flush_print(f"❌ 未找到 {name} 在 {date.date()} 的考勤记录")
            
    except Exception as e:
        flush_print(f"❌ 更新考勤记录时出错: {e}")

def process_overtime_records():
    """处理加班记录，更新考勤结果"""
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()
    
    try:
        # 获取所有加班记录
        cursor.execute("SELECT 姓名, 开始时间, 结束时间, 加班时长小时, 数据来源 FROM overwork")
        overwork_records = cursor.fetchall()
        
        flush_print(f"✅ 获取到 {len(overwork_records)} 条加班记录")
        
        # 处理每条加班记录
        for name, start_time, end_time, duration, source in overwork_records:
            try:
                # 解析时间
                start_date = datetime.strptime(start_time.split()[0], '%Y-%m-%d')
                end_date = datetime.strptime(end_time.split()[0], '%Y-%m-%d')
                
                # 解析时长
                overtime_hours = float(duration.replace('小时', '').replace('h', ''))
                
                # 更新考勤记录
                update_attendance_for_overtime(cursor, name, start_date, overtime_hours, source)
                
            except Exception as e:
                flush_print(f"❌ 处理加班记录时出错: {e}")
        
        conn.commit()
        flush_print("✅ 考勤记录更新完成")
        
    except Exception as e:
        flush_print(f"❌ 程序执行出错: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()

def main():
    process_overtime_records()

if __name__ == "__main__":
    main()