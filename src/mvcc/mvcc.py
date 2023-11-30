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
    
class Transaction():
  # Assumming TS(X) = TX (for the beginning of the transaction)
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
    
    self.ts = 1
    
    # Initialize timestamp and resources at the beginning
    # Parsing
    for op in self.input_list:
      trans = op[0]
      transaction_number = int(op[1])
      resource = ""
      
      if (transaction_number > self.ts):
        self.ts = transaction_number
      
      if (trans != "C"):
        resource = op[3]
        
        if (resource not in self.resources.keys()):
          self.resources[resource] = Resource()
      
      # Check if the beginning of transaction
      if (not self.isTransactionExists(transaction_number)): # Beginning of transaction
        self.transaction.append(Transaction(transaction_number))
    
    while self.input_list:
      action = self.input_list.pop(0)
      self.assignFunction(action)
      
    # # Do the remaining queue
    # while (self.queue):
    #   transaction = self.queue.pop(0)
    #   self.parse(transaction)
      
    print("\nResults: ")
    self.getResult() 
  
  def isTransactionExists(self, t):
    for transaction in self.transactions:
      if (transaction.transaction_id == t):
        return True
    
    return False
    
  def assignFunction(self, operation):
    trans = operation[0]
    transaction_number = int(operation[1])
    resource = ""
    
    if (trans != "C"):
      resource = operation[3]
      
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
    latest_version = self.getLatestVersionOfQk(resource)
            
    if (latest_version.r_ts < transaction_ts):
      # If R_TS(Qk) < TS(transaction)
      # then, R_TS(Qk) = TS(transaction)
      print(f"R-TS({resource}) < TS(T{transaction_number})")
      print(f"Updating R-TS({resource}) from {latest_version.r_ts} to {transaction_ts}")
      latest_version.r_ts = transaction_ts
      
      self.updateVersionOfQk(resource, latest_version)
      
    self.completed_action.append(operation)
    print(f"T{transaction_number} reads {resource}")
    
  def write(self, operation, resource, transaction_number):
    current_transaction = self.getTransaction(transaction_number)
    
    # TS of transaction
    transaction_ts = current_transaction.getTimestamp()

    # TS of Qk (find the latest version)
    latest_version = self.getLatestVersionOfQk(resource)
    
    if (transaction_ts < latest_version.r_ts):
      # If TS(transaction) < R_TS(Qk)
      # then, Rollback the transaction (Abort)
      
      self.completed_action.append(f"A{transaction_number}")
      print(f"TS(T{transaction_number}) < R-TS({resource})")
      print(f"Abort T{transaction_number})")
      
      aborted_operations = []
      for op in self.completed_action:
        if f"R{transaction_number}" in op or f"W{transaction_number}" in op:
          aborted_operations.append(op)
          
      # Restart transaction in new timestamp
      current_transaction.setTimestamp(self.ts)
      
      # Abort and rollback
      # Restart transaction operation from start till now
      for op in aborted_operations:
        tran
      
          
    
  def getLatestVersionOfQk(self, resource):
    resource = self.getResource(resource)
    latest_version = None
    for version in resource.versions:
      if version.w_ts <= transaction_ts:
        if not latest_version:
          latest_version = version
        else:
          if (version.w_ts > latest_version.w_ts):
            latest_version = version
            
    return latest_version
  
  def updateVersionOfQk(self, resource, newVersion):
    resource = self.resources[resource]
    
    resource.versions[-1] = newVersion
      
  def isTransactionBegin(self, transaction_number):
    for tt_obj in self.transaction:
      if tt_obj.transaction_id == transaction_number:
        return True
        break
    
    return False
  
  def getTransaction(self, transaction_number):
    for tt_obj in self.transaction:
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