#!/usr/bin/env python3
import sqlite3

conn = sqlite3.connect('output/interior design/pinterest.db')
cursor = conn.cursor()

cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
tables = cursor.fetchall()

print('数据库中的表:')
for table in tables:
    print(f'  {table[0]}')
    cursor.execute(f'PRAGMA table_info({table[0]})')
    columns = cursor.fetchall()
    for col in columns:
        print(f'    {col[1]} ({col[2]})')
    print()

conn.close()
