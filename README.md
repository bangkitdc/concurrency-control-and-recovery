# Concurrency Control and Recovery

Concurrency control simulation is a simulation of how DBMS works. There are consist of three protocols, Two-Phase Locking (automatic acquisition of locks, wound-wait deadlock prevention), Optimistic Concurrency Control, and Multiversion Timestamp Ordering.

## Authors
|              Name              |   NIM    |
| :----------------------------: | :------: |
| Athif Nirwasito                | 13521053 |
| Muhammad Bangkit Dwi Cahyono   | 13521055 |
| Husnia Munzayana               | 13521077 |
| Cetta Reswara Parahita         | 13521133 |

## Requirements
1. Python
2. Clone this repository

## How to Use

1. twopl.py and mvcc.py
```
Go to ./twopl or ./mvcc
usage: python {twopl.py, mvcc.py} [pathfile]

Concurrency Control Protocol Implementation

positional arguments:
  {twopl.py, mvcc.py}  choose concurrency control protocol
  [pathfile]           path file for the testcase, use file in ./test

example:
  python twopl.py twopl_tc1.txt
  python mvcc.py mvcc_tc1.txt
```

2. occ.py

Our program accepts input in the form of `R1(X); W2(Y); W2(Z); W3(A); W3(B); C1; C2; C3;`

## Copyright
2023 © K01/10. All Rights Reserved.
