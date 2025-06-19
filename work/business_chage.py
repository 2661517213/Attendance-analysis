import psycopg2
from datetime import datetime
from psycopg2 import sql

# 数据库连接配置
DB_CONFIG = {
    "host": "192.168.1.66",
    "port": "7432",
    "database": "dingding",
    "user": "root",
    "password": "123456"
}

def get_db_connection():
    """创建数据库连接"""
    return psycopg2.connect(**DB_CONFIG)

def get_business_records(cursor):
    """获取所有出差记录"""
    query = """
    SELECT 姓名, 开始时间, 结束时间, 出差事由
    FROM business
    """
    cursor.execute(query)
    return cursor.fetchall()

def update_attendance_record(cursor, name, start_date, end_date, business_reason):
    """更新指定日期范围内的考勤记录"""
    # 将日期字符串转换为datetime对象
    start_date = datetime.strptime(start_date.split()[0], '%Y-%m-%d')
    end_date = datetime.strptime(end_date.split()[0], '%Y-%m-%d')
    
    # 获取需要更新的列名（第X天）
    start_day = int(start_date.strftime('%d'))
    end_day = int(end_date.strftime('%d'))
    
    # 构建更新语句并直接更新
    for day in range(start_day, end_day + 1):
        column_name = f'第{day}天'
        
        # 直接更新考勤状态
        update_query = sql.SQL("""
            UPDATE attendance_result 
            SET {} = %s
            WHERE 姓名 = %s
        """).format(sql.Identifier(column_name))
        
        cursor.execute(update_query, (f"出差({business_reason})", name))

def main():
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        # 获取所有出差记录
        business_records = get_business_records(cursor)
        print(f"✅ 获取到 {len(business_records)} 条出差记录")
        
        # 处理每条出差记录
        for record in business_records:
            name, start_time, end_time, reason = record
            print(f"正在处理 {name} 的出差记录: {start_time} -> {end_time}")
            
            update_attendance_record(cursor, name, start_time, end_time, reason)
        
        # 提交事务
        conn.commit()
        print("✅ 所有出差记录处理完成")
        
    except Exception as e:
        print(f"❌ 处理出错: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    main()