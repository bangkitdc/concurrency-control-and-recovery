TRANSACTION_ID = 1
OPERATION_ID = 0
RESOURCE_ID = 2
FINISH = "finish"
START = "start"


from typing import Dict, Type
from Transaction import Transaction
import argparse
import re


class OCC():
    T: Dict[int, Type[Transaction]]
    def __init__(self):
        self.T = {} # List of transaction
        self.rollQ = []
        self.cmdQ = []
        self.time = 0
    def txExist(self, tx_id) -> bool:
        return tx_id in self.T.keys()
    
    def checkTX(self, tx_id) -> None:
        if not self.txExist(tx_id):
            newTX = Transaction(tx_id)
            newTX['start'] = self.time
            self.T[tx_id] = newTX
            
    def write(self, tx_id):
        for resource in self.T[tx_id].writeset:
            print(f"Write {resource} from transaction-{tx_id}")
    def read(self, tx_id, resource) -> None:
        

        self.T[tx_id].readset.add(resource)
        print(f"Read {resource} on transaction-{tx_id}.\nTransaction-{tx_id} readset: {self.T[tx_id].readset}")
    
    def local_write(self,tx_id,resource)->None:
        self.T[tx_id].writeset.add(resource)
        print(f"Write locally {resource} on transaction-{tx_id}.\nTransaction-{tx_id} writeset: {self.T[tx_id].writeset}")
        
        
    def commit(self, tx_id) -> None:
        self.T[tx_id]['finish'] = self.time
        print(f"Commit transaction-{tx_id}")
    
    def abort(self,tx_id) -> None:
        self.cmdQ.extend(self.T[tx_id].history)
        self.T[tx_id].rollback()
        print(f"Abort Transaction-{tx_id}")
        
        
    def valid_cond_1(self, tx_id, other_tx:Transaction) -> bool:
        return other_tx['finish'] < self.T[tx_id]['start']
    def valid_cond_2(self,tx_id, other_tx: Transaction) -> bool:
        return (self.T[tx_id]['start'] < other_tx['finish'] < self.T[tx_id]['validation']) and (len(other_tx.writeset.intersection(self.T[tx_id].readset))==0)
    
    def validate(self, tx_id) -> bool:
        
        # Validate Tj succed for all TI with TS(Ti)<TS(Tj)
        # The following holds
        
        # 1. finishTS(Ti) < startTS(Tj)
        # 2. startTS(Tj) < finishTS(Ti) <  validation(Tj) 
        #    and write set of Ti does not intersect with read set of Tj
        self.T[tx_id]['validation'] = self.time
        valid = True
        for transaction in self.T.values():
            if self.T[tx_id]['validation']<=transaction['validation']:
                continue
            
            if not( (self.valid_cond_1(tx_id, transaction)) or (self.valid_cond_2(tx_id,transaction))):
                
                valid = False
                print("Validation failed")
                break  
        if valid:
            #Validation succeed
            #Commit
            self.write(tx_id)
            self.commit(tx_id)
        else:
            #Validation failed
            #Rollback
            self.abort(tx_id)
            
                
    def parse(self, text:str)->None:
        sequence = text.replace(" ", "").split(";")
        for action in sequence:
            if(action[0].upper()=='C'):
                self.cmdQ.append([action[0].upper(),action[1:],None])
                continue
            print(action)
            bropen = action.index('(')
            brclose = action.index(')')
            #validate string format, throw exceeption if invalid
            if (action[0].upper() !='R' and action[0].upper()!='W')\
                or (bropen<1 or brclose<0)\
                or    (brclose-bropen<2)\
                    or (len(re.findall('(')!=1) and len(re.findall(')'!=1)) ):#Check if there are only one bracket each
                     
                raise "Input Invalid"
                # [operation, transactionID, resource]
            self.cmdQ.append([action[0].upper(), action[1:bropen],action[bropen+1:brclose]])
            
            
            
    def run(self):
        #Execute
        while len(self.cmdQ)>0:
            
            self.time +=1
            print(f"\nTimestamp: {self.time}")
            action = self.cmdQ.pop(0)
            op = action[OPERATION_ID]
            tx = action[TRANSACTION_ID]
            self.checkTX(tx)
            self.T[tx].history.append(action)
            if op == 'R':
                #do read
                self.read(tx,action[RESOURCE_ID])
            elif op == 'W':
                #do write
                self.local_write(tx,action[RESOURCE_ID])
            elif op == 'C':
                #do commit     
                self.validate(tx)
            else:
                raise f"Invalid command: '{op}'"

            
                



# parser = argparse.ArgumentParser(prog='Optimistic Concurency Control',description="This program simulates optimistic concurency control from read, write, and commit input")
try:
    occ = OCC()
    # parser.add_argument('input')
    # args = vars(parser.parse_args())
    # occ.parse("R1(B);R2(B);W2(B);R2(A);W2(A);R1(A);R2(A);C1;C2")
    # occ.parse("R1(users);R2(users);W2(users);W1(users);C2;C1")
    occ.parse("R1(X);R2(Z);R1(Z);R3(X);R3(Y);W1(X);W3(Y);R2(Y);W2(Z);W2(Y);C1;C2;C3")
    
    occ.run()   
except Exception as e:
    print(str(e)) 
    
    
    
