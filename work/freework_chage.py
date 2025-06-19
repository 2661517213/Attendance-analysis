import psycopg2
from datetime import datetime
from psycopg2 import sql
from config import DB_CONFIG


def get_db_connection():
    """创建数据库连接"""
    return psycopg2.connect(**DB_CONFIG)

def get_freework_records(cursor):
    """获取所有请假记录，并处理特殊姓名标识"""
    query = """
    SELECT 
        CASE 
            WHEN 姓名 LIKE '%CDTL' THEN LEFT(姓名, LENGTH(姓名)-4)
            ELSE 姓名
        END as 姓名,
        开始时间, 
        结束时间, 
        请假说明, 
        时长,
        数据来源
    FROM freework
    """
    cursor.execute(query)
    return cursor.fetchall()

def update_attendance_record(cursor, name, start_date, end_date, leave_reason, duration, source):
    """更新指定日期范围内的考勤记录，将请假信息追加到现有记录后"""
    try:
        # 将日期字符串转换为datetime对象
        start_date = datetime.strptime(start_date.split()[0], '%Y-%m-%d')
        end_date = datetime.strptime(end_date.split()[0], '%Y-%m-%d')
        
        # 获取需要更新的列名（第X天）
        start_day = int(start_date.strftime('%d'))
        end_day = int(end_date.strftime('%d'))
        
        # 构建更新语句并追加更新
        for day in range(start_day, end_day + 1):
            column_name = f'第{day}天'
            
            # 修改查询语句，使用 LIKE 进行模糊匹配
            check_query = sql.SQL("""
                SELECT {} FROM attendance_result 
                WHERE 姓名 LIKE %s || '%%'
            """).format(sql.Identifier(column_name))
            
            cursor.execute(check_query, (name,))
            result = cursor.fetchone()
            
            if not result:
                print(f"警告: 未找到员工 {name} 的记录")
                continue
                
            current_value = result[0]
            
            # 生成请假信息
            leave_info = f"\n{source}请假({duration})({leave_reason})"
            
            # 如果当前值存在，则追加；否则直接设置（移除开头的换行符）
            if current_value and current_value.strip():
                new_value = f"{current_value}{leave_info}"
            else:
                new_value = leave_info.lstrip('\n')
            
            # 更新记录时也使用模糊匹配
            update_query = sql.SQL("""
                UPDATE attendance_result 
                SET {} = %s
                WHERE 姓名 LIKE %s || '%%'
            """).format(sql.Identifier(column_name))
            
            cursor.execute(update_query, (new_value, name))
            
    except Exception as e:
        print(f"处理 {name} 的请假记录时出错: {e}")
        raise e

def main():
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        # 获取所有请假记录
        freework_records = get_freework_records(cursor)
        print(f"✅ 获取到 {len(freework_records)} 条请假记录")
        
        # 处理每条请假记录
        for record in freework_records:
            name, start_time, end_time, reason, duration, source = record
            print(f"正在处理 {name} 的请假记录: {start_time} -> {end_time}")
            
            try:
                update_attendance_record(cursor, name, start_time, end_time, reason, duration, source)
            except Exception as e:
                print(f"❌ 处理失败: {e}")
                continue
        
        # 提交事务
        conn.commit()
        print("✅ 所有请假记录处理完成")
        
    except Exception as e:
        print(f"❌ 程序执行出错: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    main()