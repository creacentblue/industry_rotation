# -*- coding: utf-8 -*-

from rqalpha import run_file
import pickle

config = {
  "base": {
    "start_date": "2014-02-21 00:00:00",
    "end_date": "2018-02-28 00:00:00",
    "frequency": "1d",
    "benchmark": "000300.XSHG",
    "accounts": {
      "stock": 100000000,
    }
  },
  "extra": {
    "log_level": "verbose",
  },
  "mod": {
    "sys_analyser": {
      "enabled": True,
      "plot": True,
      "output_file": "results.pkl"
    },
    "sys_progress": {
          "enabled": True,
          "show": True,
    }
  }

}

strategy_file_path = "strategy.py"

run_file(strategy_file_path, config)
