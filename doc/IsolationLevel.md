# Isolation Level

## Brief

| Level            | Dirty Read | Non-repeatable Read | Phantom Read |
|------------------|:----------:|:-------------------:|:------------:|
| Read uncommitted |     Yes    |         Yes         |      Yes     |
| Read committed   |     No     |         Yes         |      Yes     |
| Repeatable read  |     No     |          No         |      Yes     |
| Serializable     |     No     |          No         |      No      |

- What is `Dirty Read`?

|                Txn1                |                  Txn2                  |
|:----------------------------------:|:--------------------------------------:|
|                begin               |                  begin                 |
|                                    | update table set age = 10 where id = 1 |
| select age from table where id = 1 |                                        |
|               commit               |                 commit                 |

txn2 modifies data but not commit, which is read by txn1, then txn2 aborts, so txn1 reads dirty data.

- What is `Non-repeatable Read`?

|                Txn1                |                  Txn2                  |
|:----------------------------------:|:--------------------------------------:|
|                begin               |                  begin                 |
| select age from table where id = 1 |                                        |
|                                    | update table set age = 10 where id = 1 |
|                                    |                 commit                 |
| select age from table where id = 1 |                                        |
|               commit               |                                        |

txn1 reads data, txn2 modifies data and commit, txn1 reads data again, and the data read twice by txn1 is inconsistent.

- What is `Phantom Read`?

|                Txn1                |                   Txn2                   |
|:----------------------------------:|:----------------------------------------:|
|                begin               |                   begin                  |
| select age from table where id > 2 |                                          |
|                                    | insert into table(id, age) values(5, 10) |
|                                    |                  commit                  |
|  select age from table where id >2 |                                          |
|               commit               |                                          |

txn1 counts the number of rows, txn2 inserts data and commit, txn1 counts the number of rows again, and the number of rows counted by txn1 is inconsistent.

## Solution of lock

- `READ UNCOMMITED` only takes a write lock when needed.

- `READ COMMITTED` to solve the problem of dirty reading, the solution is to lock the read when reading, and then unlock the read after reading; lock the write when writing, **but not unlock until the commit finishing**; In this way, uncommitted data will never be read because there is a write lock on it.

- `REPEATABLE READ` to solve the problem of no-repeatable reads. The transaction does not want to be disturbed by the writing of other transactions in the middle of reading data twice, which requires the use of a **Two-Phase Lock(2PL)**: the transaction is divided into two stages (commit/abort is not considered), and the locking stage (GROWING) is only locked, and the unlocking stage (SHINKING) is only unlocked. In this way, when reading the second time, the read lock of the previous reading must still be there, avoiding the modification in the middle.

