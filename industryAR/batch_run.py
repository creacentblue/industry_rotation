# -*- coding: utf-8 -*-

from rqalpha import run_file
import pickle


tasks = []
for con in range(2, 4):
    for mon in range(6):
        for num in range(3, 29, 3):
            config = {
                "base": {
                    "start_date": "2016-01-29 00:00:00",
                    "end_date": "2018-04-15 00:00:00",
                    "frequency": "1d",
                    "benchmark": "000300.XSHG",
                    "accounts": {
                        "stock": 100000000,
                    }
                },
                "extra": {
                    "context_vars": {
                        "num": num,
                        "mon": mon,
                        "con": con,
                    },
                    "log_level": "error",
                },
                "mod": {
                    "sys_progress": {
                        "enabled": True,
                        "show": True,
                    },
                    "sys_analyser": {
                        "enabled": True,
                        "output_file": r"./con-mon-ind/{con}-{mon}-{num}.pkl".format(
                            num=num, mon=mon+1, con=con+1
                        )
                    },
                },

            }
            tasks.append(config)


i = 0
strategy_file_path = "AR_strategy.py"
for task in tasks:
    run_file(strategy_file_path, task)
    print(i)
    i += 1
