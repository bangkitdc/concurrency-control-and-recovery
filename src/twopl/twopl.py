import sys

from typing import List

SL = "shared_lock"
XL = "exclusive_lock"

ABORTED = "aborted"
WAITING = "waiting"
COMMITTED = "committed"
ACTIVE = "active"

class LockTable():
  def __init__(self, locked_data_item, transaction_id, lock_state):
    self.locked_data_item = locked_data_item
    self.lock_state = lock_state
    
    self.lock_held_by = []
    self.lock_held_by.append(transaction_id)
    
  def addLockHeld(self, transaction_id):
    self.lock_held_by.append(transaction_id)

class TransactionTable():
  def __init__(self, transaction_id, timestamp, transaction_state):
    self.transaction_id = transaction_id
    self.timestamp = timestamp
    self.transaction_state = transaction_state
    
    self.locked_resources = []
    self.waiting_for_transaction: int = None
    
  def changeTransactionState(self, state):
    self.transaction_state = state
  
  def addLockedResource(self, resource):
    self.locked_resources.append(resource)

class TwoPL():
  def __init__(self, input_list: List = []):
    self.input_list = input_list
    self.completed_action = []
        
    self.lock_table_objects = []
    self.transaction_table_objects = []
    self.queue = []   
    
  def run(self):
    print("Two Phase Locking (with Wound-Wait): ")
    
    while self.input_list:
      action = self.input_list.pop(0)
      self.parse(action)
      
    # Do the remaining queue
    while (self.queue):
      transaction = self.queue.pop(0)
      self.parse(transaction)
      
    print("\nResults: ")
    self.getResult() 
  
  def isTransactionExists(self, t):
    for transaction in self.transaction_table_objects:
      if (transaction.transaction_id == t):
        return True
    
    return False
  
  def parse(self, operation: str):    
    # trans : {R, W, C}
    trans = operation[0]
    transaction_number = int(operation[1])
    resource = ""
    
    if (trans != "C"):
      resource = operation[3]
    
    # Check if the beginning of transaction
    if (not self.isTransactionExists(transaction_number)): # beginning of transaction
      timestamp = len(self.transaction_table_objects) + 1
      self.transaction_table_objects.append(TransactionTable(transaction_number, timestamp, ACTIVE))
    
    self.assignFunction(operation)
    
  def assignFunction(self, operation):
    trans = operation[0]
    transaction_number = int(operation[1])
    resource = ""
    
    if (trans != "C"):
      resource = operation[3]
    
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
      
  def isTransactionBegin(self, transaction_number):
    for tt_obj in self.transaction_table_objects:
      if tt_obj.transaction_id == transaction_number:
        return True
        break
    
    return False
  
  def getTransaction(self, transaction_number):
    for tt_obj in self.transaction_table_objects:
      if tt_obj.transaction_id == transaction_number:
        return tt_obj
      
  def getResult(self):
    res = "; ".join(self.completed_action)
      
    print(res)
    
  def woundWait(self, request_transaction, holding_transaction, operation):
    if (request_transaction.timestamp < holding_transaction.timestamp):
      # Wound
      # Old transaction forces the new transaction to rollback
      holding_transaction.changeTransactionState(ABORTED)
      print(f"T{request_transaction.transaction_id} forces T{holding_transaction.transaction_id} to rollback")
      
      self.completed_action.append(f"A{holding_transaction.transaction_id}")
      
      # Unlock all the resources from the new transaction
      self.unlock(holding_transaction.transaction_id, True)
      return True
    else:
      # Add request transaction to queue
      request_transaction.changeTransactionState(WAITING)
    
      print(f"T{request_transaction.transaction_id} waiting for T{holding_transaction.transaction_id}")
      request_transaction.waiting_for_transaction = holding_transaction.transaction_id
      
      if (not self.checkDuplicateOperationQueue(operation)): # If not in queue
        self.queue.append(operation)
      
      self.printQueue()  
      return False

  def checkDuplicateOperationQueue(self, operation):    
    for op in self.queue:
      if op == operation:
        return True
    return False
    
  def read(self, operation, resource, transaction_number):
    current_transaction = self.getTransaction(transaction_number)
    
    # If current transaction is ABORTED or WAITING
    if (current_transaction.transaction_state == ABORTED or current_transaction.transaction_state == WAITING):
      # Put in the back of the queue
      self.queue.append(operation)
      
      print("Can't read transaction, insert into queue")
            
    else:
      length = len(self.lock_table_objects)
      isResourceUsed = False
      
      # Check lock table
      if (length != 0):
        for lt_obj in self.lock_table_objects:
          if (lt_obj.locked_data_item == resource):
            isResourceUsed = True
            
            if (lt_obj.lock_state == XL):
              print(f"Conflict, {resource} is under XL by T{lt_obj.lock_held_by[0]}")
              
              # Check timestamp 
              if (self.woundWait(current_transaction, 
                             self.getTransaction(lt_obj.lock_held_by[0]), 
                             f"R{transaction_number}({resource})")):
                lt_obj.addLockHeld(transaction_number)
                current_transaction.addLockedResource(resource)
                
                print(f"T{transaction_number} grant SL({resource})")
                self.completed_action.append(f"SL{transaction_number}({resource})")
                self.completed_action.append(operation)
              
            elif (lt_obj.lock_state == SL):
              # Add transaction to the list of transactions that holds SL on the resource
              lt_obj.addLockHeld(transaction_number)
              current_transaction.addLockedResource(resource)
              
              print(f"T{transaction_number} grant SL({resource})")
              self.completed_action.append(f"SL{transaction_number}({resource})")
              self.completed_action.append(operation)
        
        if (not isResourceUsed):
          self.lock_table_objects.append(LockTable(resource, transaction_number, SL))
          current_transaction.addLockedResource(resource)
          
          print(f"T{transaction_number} grant SL({resource})")
          self.completed_action.append(f"SL{transaction_number}({resource})")
          self.completed_action.append(operation)
          
      else:
        # Add new resource to lock table
        self.lock_table_objects.append(LockTable(resource, transaction_number, SL))
        current_transaction.addLockedResource(resource)
        
        print(f"T{transaction_number} grant SL({resource})")
        self.completed_action.append(f"SL{transaction_number}({resource})")
        self.completed_action.append(operation)

  def write(self, operation, resource, transaction_number):
    current_transaction = self.getTransaction(transaction_number)
    
    # If current transaction is ABORTED or WAITING
    if (current_transaction.transaction_state == ABORTED or current_transaction.transaction_state == WAITING):
      print("Can't write transaction, insert into queue")
      # Put in the back of the queue
      self.queue.append(operation)
      
      self.printQueue()
            
    else:
      length = len(self.lock_table_objects)
      isResourceUsed = False
      
      if (length != 0):
        for lt_obj in self.lock_table_objects:
          if (lt_obj.locked_data_item == resource):
            isResourceUsed = True
            
            if (lt_obj.lock_state == XL):
              print(f"Conflict, {resource} is under XL by T{lt_obj.lock_held_by[0]}")
              
              # Check timestamp 
              if (self.woundWait(current_transaction, 
                             self.getTransaction(lt_obj.lock_held_by[0]), 
                             f"W{transaction_number}({resource})")):
                lt_obj.addLockHeld(transaction_number)
                current_transaction.addLockedResource(resource)
                
                print(f"T{transaction_number} grant XL({resource})")
                self.completed_action.append(f"XL{transaction_number}({resource})")
                self.completed_action.append(operation)
              
            elif (lt_obj.lock_state == SL):
              # If it's just held by 1 lock
              if (len(lt_obj.lock_held_by) == 1):
                
                # Check if the resource is locked under the same transaction
                if (lt_obj.lock_held_by[0] == transaction_number):
                  lt_obj.lock_state = XL
                  print(f"T{transaction_number} Upgrading SL to XL on {resource}")
                  self.completed_action.append(f"XL{transaction_number}({resource})")
                  self.completed_action.append(operation)
                else:
                  # Check timestamp 
                  if (self.woundWait(current_transaction, 
                                self.getTransaction(lt_obj.lock_held_by[0]), 
                                f"W{transaction_number}({resource})")):
                    lt_obj.addLockHeld(transaction_number)
                    current_transaction.addLockedResource(resource)
                    
                    print(f"T{transaction_number} grant XL({resource})")
                    self.completed_action.append(f"XL{transaction_number}({resource})")
                    self.completed_action.append(operation)
                    
                    self.startAbortedTransaction(self.getTransaction(lt_obj.lock_held_by[0]).transaction_id)
                
              else: # More than one lock
                # Wound to all transactions
                for index, held_by in enumerate(lt_obj.lock_held_by):
                  if (held_by != transaction_number):
                    # Check timestamp 
                    if(self.woundWait(current_transaction, 
                                  self.getTransaction(held_by), 
                                  f"W{transaction_number}({resource})")):
                      if (transaction_number in lt_obj.lock_held_by):
                        # Find lt obj
                        for held_by_2 in lt_obj.lock_held_by:
                          if (held_by_2 == transaction_number):
                            lt_obj.lock_state = XL
                            print(f"T{transaction_number} Upgrading SL to XL on {resource}")
                            self.completed_action.append(f"XL{transaction_number}({resource})")
                            self.completed_action.append(operation)
                            
                            self.startAbortedTransaction(held_by)
                                                  
                      else:
                        lt_obj.addLockHeld(transaction_number)
                        current_transaction.addLockedResource(resource)
                        
                        print(f"T{transaction_number} grant XL({resource})")
                        self.completed_action.append(f"XL{transaction_number}({resource})")
                        self.completed_action.append(operation)
                    
                    # TODO: Handle wound to more than one transaction
        
        if (not isResourceUsed):
          self.lock_table_objects.append(LockTable(resource, transaction_number, XL))
          current_transaction.addLockedResource(resource)
          
          print(f"T{transaction_number} grant XL({resource})")
          self.completed_action.append(f"XL{transaction_number}({resource})")
          self.completed_action.append(operation)
          
      else:
        self.lock_table_objects.append(LockTable(resource, transaction_number, XL))
        current_transaction.addLockedResource(resource)
        
        print(f"T{transaction_number} grant XL({resource})")
        self.completed_action.append(f"XL{transaction_number}({resource})")
        self.completed_action.append(operation)
        
  def commit(self, operation, transaction_number):
    # Check first 
    current_transaction = self.getTransaction(transaction_number)
    
    # If current transaction is ABORTED or WAITING
    if (current_transaction.transaction_state == ABORTED or current_transaction.transaction_state == WAITING):
      print("Can't commit transaction, insert into queue")
      # Put in the back of the queue
      self.queue.append(operation)
      
      self.printQueue()
    else:   
      # Check queue first
      for op in self.queue:
        if f"R{transaction_number}" in op or f"W{transaction_number}" in op:
          self.queue.append(f"C{transaction_number}")
          return
        
      for transaction in self.transaction_table_objects:
        if (transaction.transaction_id == transaction_number and transaction.transaction_state != ABORTED):
          print(f"Commiting T{transaction_number}")
          transaction.changeTransactionState(COMMITTED)
          
          self.completed_action.append(f"C{transaction_number}")
      
      current_transaction = self.getTransaction(transaction_number)
      for t in self.transaction_table_objects:
        if (t.waiting_for_transaction == transaction_number):
          t.waiting_for_transaction = None
          t.changeTransactionState(ACTIVE)
      
      self.unlock(transaction_number, False)
      
  def unlock(self, transaction_number, wound):
    print(f"Unlocking all resources held by T{transaction_number}")
    
    resource_unlocked_list = []
        
    for transaction in self.transaction_table_objects:
      if (transaction.transaction_id == transaction_number):
        for lock in transaction.locked_resources:
          for resource in self.lock_table_objects:
            if (resource.locked_data_item == lock):
              resource_unlocked_list.append(resource.locked_data_item)
              self.completed_action.append(f"UL{transaction.transaction_id}({resource.locked_data_item})")
              if (len(resource.lock_held_by) == 1):
                # If the same resource is held by multiple transactions,
                # remove this transaction from list of transactions
                # holding the lock
                self.lock_table_objects.remove(resource)
              else:
                # If only this transaction that has any lock in this resource
                # remove resource from lock table completely
                resource.lock_held_by.remove(transaction_number)
    
    if (wound):
      # do nothing
      print("", end="")
    else:
      self.startWaitingTransactions(transaction_number, resource_unlocked_list)
  
  def startAbortedTransaction(self, transaction_number_aborted):
    aborted_operations = []
    
    for op in self.completed_action:
      if f"R{transaction_number_aborted}" in op or f"W{transaction_number_aborted}" in op:
        aborted_operations.append(op)
    
    # Change status
    current_transaction = self.getTransaction(transaction_number_aborted)
    current_transaction.changeTransactionState(ACTIVE)
    
    for op in aborted_operations:
      self.assignFunction(op)
  
  def startWaitingTransactions(self, transaction_number, resource_unlocked_list):
    print("Checking for queue")
    self.printQueue()
    
    for op in self.queue:
      trans = op[0]
      transaction_number_waiting = int(op[1])
      resource = ""
      
      if (trans != "C"):
        resource = op[3]
        
      transaction = self.getTransaction(transaction_number_waiting)
              
      for resource_unlocked in resource_unlocked_list:
        if (resource == resource_unlocked):
          self.queue.remove(op)
          self.assignFunction(op)
          
  def printQueue(self):
    queue = "Current queue = "
    queue += "["
    queue_str = ", ".join(self.queue)
      
    queue += queue_str
      
    queue += "]"
    
    print(queue)
          
if __name__ == "__main__":
  if (len(sys.argv)) != 2:
    print("Usage: python twopl.py [input.txt]")
  else:
    input_file = sys.argv[1]
    with open(f"../test/{input_file}", 'r') as file:
      input_list = [line.rstrip(";\n") for line in file]
        
    print(input_list)
  
  # main = TwoPL(['R1(X)', 'R2(X)', 'R3(Y)', 'R1(Y)', 'W2(X)', 'C2', 'R3(Z)', 'W1(Y)', 'C1', 'C3'])
  # main = TwoPL(['R1(A)', 'R2(A)', 'R3(B)', 'W1(A)', 'R2(C)', 'R2(B)', 'C3', 'W2(B)' , 'C2', 'W1(C)', 'C1'])
  # main = TwoPL(['R1(A)', 'W2(A)', 'C1', 'R3(B)', 'C3', 'R2(B)', 'C2'])
  main = TwoPL(input_list)
  main.run()