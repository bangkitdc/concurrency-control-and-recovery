# Transaction.py
import numpy as np
from typing import Set, List
class Transaction():
    """Attributes"""
    readset: Set[str]
    writeset: Set[str]
    history: list
    """Method"""
    def __init__(self, idx):
        self.idx  = idx
        self.readset = set()
        self.writeset = set()
        self.timestamps = {'start':np.inf, 'validation':np.inf, 'finish':np.inf}
        self.history = []
    def __getitem__(self, index):
        return self.timestamps[index]
    def  __setitem__(self, key, value):
        self.timestamps[key] = value
        
    def rollback(self)->None:
        self.readset = set()
        self.writeset = set()
        self.timestamps = {'start':np.inf, 'validation':np.inf, 'finish':np.inf}
        self.history = []
    
    