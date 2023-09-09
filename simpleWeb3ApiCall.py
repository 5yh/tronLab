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
hash1='0x6c2aed1b8d1c814e8d0cf6e93d940572455ac0d327275bdb54d1ec629e26a59c'
content = w3.eth.get_transaction_receipt(hash1)
temp = w3.eth.get_transaction(hash1)
print(content)
print(temp)