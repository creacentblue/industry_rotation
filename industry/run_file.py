# -*- coding: utf-8 -*-

from rqalpha import run_file

config = {
  "base": {
    "start_date": "2016-01-29 00:00:00",
    "end_date": "2018-06-01 00:00:00",
    "frequency": "1d",
    "benchmark": "000300.XSHG",
    "accounts": {
      "stock": 100000000
    }
  },
  "extra": {
    "log_level": "verbose",
  },
  "mod": {
    "sys_analyser": {
      "enabled": True,
      "plot": True
    },
    "sys_progress": {
          "enabled": True,
          "show": True,
    }
  }

}

strategy_file_path = "strategy3.py"

run_file(strategy_file_path, config)
