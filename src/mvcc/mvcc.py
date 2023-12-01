import sys

from typing import List

class Version():
  # (R, W)
  def __init__(self, r_ts, w_ts):
    self.r_ts = r_ts
    self.w_ts = w_ts
    
class Resource():
  def __init__(self):
    start_version = Version(0, 0)
    
    # List of versions
    self.versions = [start_version]
    
  def getVersion(self, version):
    target_version = self.versions[version]
    return f"({target_version.r_ts}, {target_version.w_ts})"
    
class Transaction():
  def __init__(self, transaction_id):
    self.transaction_id = transaction_id
    self.ts = transaction_id
    
  def getTimestamp(self):
    return self.ts
  
  def setTimestamp(self, timestamp):
    self.ts = timestamp

class MVCC():
  def __init__(self, input_list: List = []):
    self.input_list = input_list
    self.completed_action = []
        
    self.transactions = []
    self.resources = {}
    
  def run(self):
    print("Multiversion Timestamp Ordering: ")
    
    self.ts = 0
        
    # Initialize timestamp and resources at the beginning
    # Parsing
    for op in self.input_list:
      trans = op[0]
      transaction_number = int(op[1])
      resource = ""
      
      if (trans != "C"):
        resource = op[3]
        
        if (resource not in self.resources.keys()):
          self.resources[resource] = Resource()
      
      # Assumming TS(X) = TX (for the beginning of the transaction)
      # Check if the beginning of transaction
      if (not self.isTransactionExists(transaction_number)): # Beginning of transaction
        self.transactions.append(Transaction(transaction_number))
    
    while self.input_list:
      action = self.input_list.pop(0)
      self.assignFunction(action)
      
    print("\nResults: ")
    self.getResult() 
  
  def isTransactionExists(self, t):
    for transaction in self.transactions:
      if (transaction.transaction_id == t):
        return True
    
    return False
    
  def assignFunction(self, operation, update_ts=True):
    trans = operation[0]
    transaction_number = int(operation[1])
    resource = ""
    
    if (trans != "C"):
      resource = operation[3]
      
    if (update_ts):
      self.ts += 1
    
    print()
    print(operation)
    if trans == 'R':    
      self.read(operation, resource, transaction_number)
    elif trans == 'W':      
      self.write(operation, resource, transaction_number)
    elif trans == 'C':
      self.commit(operation, transaction_number)
    else:
      print(f"{operation} is invalid")
      print("Please input correct test case, program exiting")
      exit(1)
            
  def read(self, operation, resource, transaction_number):
    current_transaction = self.getTransaction(transaction_number)
    
    # TS of transaction
    transaction_ts = current_transaction.getTimestamp()

    # TS of Qk (find the latest version)
    latest_version = self.getLatestVersionOfQk(resource, transaction_ts)
        
    if (latest_version.r_ts < transaction_ts):
      # If R_TS(Qk) < TS(transaction)
      # then, R_TS(Qk) = TS(transaction)
      print(f"R-TS({resource}) < TS(T{transaction_number})")
      print(f"Updating R-TS({resource}) from {latest_version.r_ts} to {transaction_ts}")
      
      latest_version.r_ts = transaction_ts
      
      self.updateLastVersionOfQk(resource, latest_version)
      
      last_version = len(self.resources[resource].versions) - 1
      print(f"TS({resource}{last_version}) = {self.resources[resource].getVersion(last_version)}")
      
    self.completed_action.append(operation)
    print(f"T{transaction_number} reads {resource}")
    
  def write(self, operation, resource, transaction_number):
    current_transaction = self.getTransaction(transaction_number)
    
    # TS of transaction
    transaction_ts = current_transaction.getTimestamp()

    # TS of Qk (find the latest version)
    latest_version = self.getLatestVersionOfQk(resource, transaction_ts)
    
    if (transaction_ts < latest_version.r_ts):
      # If TS(transaction) < R_TS(Qk)
      # then, Rollback the transaction (Abort)
      
      self.completed_action.append(f"A{transaction_number}")
      print(f"TS(T{transaction_number}) < R-TS({resource})")
      print(f"Abort T{transaction_number}")
      
      aborted_operations = []
      check_for_cascading = False
      rollback_transactions = []
      
      for index, op in enumerate(self.completed_action):
        if f"R{transaction_number}" in op or f"W{transaction_number}" in op:
          aborted_operations.append(op)
          
          if (f"W{transaction_number}" in op and not check_for_cascading):
            # Check for cascading rollback
            check_for_cascading = True
            rollback_transactions = self.getCascadingRollbackTransaction(index, op)
      
      aborted_operations.append(operation)
      
      # Iterate through rollback transactions
      for rollback_i in rollback_transactions:
        for op in self.completed_action:
          if f"R{rollback_i}" in op or f"W{rollback_i}" in op:
            aborted_operations.append(op)
            
      print("ABORTED")
      print(aborted_operations)
    
      # Restart transaction in new timestamp
      current_transaction.setTimestamp(self.ts)
      
      # Abort and rollback
      # Restart transaction operation from start till now
      for op in aborted_operations: 
        self.assignFunction(op, False) 
      return
    
    elif(transaction_ts == latest_version.w_ts):
      # If TS(transaction) = W-TS(Qk)
      # then, overwrite contents Qk
      print(f"Overwritten the contents of {resource}")
      pass
    else:
      # Create a new version Qi of Q, Update 
      print(f"Made a new version Qi with W-TS({resource}{len(self.resources[resource].versions)}) and R-TS({resource}{len(self.resources[resource].versions)}) = {transaction_ts}")
      new_version = Version(transaction_ts, transaction_ts)
      self.resources[resource].versions.append(new_version)
      
      last_version = len(self.resources[resource].versions) - 1
      print(f"TS({resource}{last_version}) = {self.resources[resource].getVersion(last_version)}")
    
    self.completed_action.append(operation)
    print(f"T{transaction_number} writes {resource}")
  
  def getCascadingRollbackTransaction(self, index, op_write):
    rollback_transaction = []
    
    for i in range(index, len(self.completed_action)):
      # Op read
      op = self.completed_action[i]
      
      trans = op[0]
      transaction_number = int(op[1])
      resource = ""
      
      if (trans == "A"):
        continue
      
      if (trans != "C"):
        resource = op[3]
        
      # Op write
      trans_write = op_write[0]
      transaction_number_write = int(op_write[1])
      resource_write = ""
      
      if (trans_write != "C"):
        resource_write = op_write[3]
        
      if (trans == "R" and resource_write == resource and transaction_number != transaction_number_write):
        rollback_transaction.append(transaction_number)
        
      # If commits, then unrecoverable transaction
      elif (trans == "C" and transaction_number in rollback_transaction):
        rollback_transaction.remove(transaction_number)
        
    return rollback_transaction
  
  def commit(self, operation, transaction_number):
    self.completed_action.append(operation)
    print(f"T{transaction_number} commits")
    
  def getLatestVersionOfQk(self, resource, transaction_ts):
    resource = self.getResource(resource)
    
    return resource.versions[-1]
  
  def updateLastVersionOfQk(self, resource, newVersion):
    resource = self.getResource(resource)
    
    resource.versions[-1] = newVersion
      
  def isTransactionBegin(self, transaction_number):
    for tt_obj in self.transactions:
      if tt_obj.transaction_id == transaction_number:
        return True
        break
    
    return False
  
  def getTransaction(self, transaction_number):
    for tt_obj in self.transactions:
      if tt_obj.transaction_id == transaction_number:
        return tt_obj
      
  def getResource(self, resource):
    return self.resources[resource]
      
  def getResult(self):
    res = "; ".join(self.completed_action)
      
    print(res)

if __name__ == "__main__":
  if (len(sys.argv)) != 2:
    print("Usage: python mvcc.py [input.txt]")
  else:
    input_file = sys.argv[1]
    with open(f"../test/{input_file}", 'r') as file:
      input_list = [line.rstrip(";\n") for line in file]
        
    print(input_list)
  
  # main = MVCC(['R1(X)', 'R2(X)', 'R3(Y)', 'R1(Y)', 'W2(X)', 'C2', 'R3(Z)', 'W1(Y)', 'C1', 'C3'])
  # main = MVCC(['R1(A)', 'R2(A)', 'R3(B)', 'W1(A)', 'R2(C)', 'R2(B)', 'C3', 'W2(B)' , 'C2', 'W1(C)', 'C1'])
  # main = MVCC(['R1(A)', 'W2(A)', 'C1', 'R3(B)', 'C3', 'R2(B)', 'C2'])
  main = MVCC(input_list)
  main.run()