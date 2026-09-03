#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""探测 SCC 对 mysql crate 初始化类语句的支持（定位 CouldNotSetupConnection 根因）。"""
import pymysql

c = pymysql.connect(host="127.0.0.1", port=3309, user="root", autocommit=True)
cur = c.cursor()
qs = [
    "SET NAMES utf8mb4",
    "SET NAMES utf8mb4 COLLATE utf8mb4_bin",
    "SET character_set_results = NULL",
    "SET autocommit=1",
    "SELECT @@version_comment",
    "SHOW VARIABLES LIKE 'character_set_client'",
    "SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ",
]
for q in qs:
    try:
        cur.execute(q)
        try:
            cur.fetchall()
        except Exception:
            pass
        print("OK  ", q)
    except Exception as e:
        print("FAIL", q, "->", type(e).__name__, str(e)[:100])
c.close()
