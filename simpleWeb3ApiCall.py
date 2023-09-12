# coding=UTF-8
from web3 import Web3, HTTPProvider
from web3.eth import Eth
from tqdm import tqdm
from collections import defaultdict
import pickle
import numpy as np
import os
import datetime
import sys
import pandas as pd
import time
import multiprocessing as mp
from multiprocessing import Lock
import argparse
# 简单web3 api调用测试，用来提取单个交易数据
w3 = Web3(Web3.HTTPProvider('http://localhost:50545/jsonrpc'))
print("trc10")
hash1='0x3354c1800c602996c227b6b10e462d26cce35e11f7df6d950b434417c4e68a13'
content = w3.eth.get_transaction_receipt(hash1)
temp = w3.eth.get_transaction(hash1)
print(content)
print(temp)