# Wound-wait and Wait-die

## Brief

- **Timestamp(T<sub>n</sub>) < Timestamp(T<sub>k</sub>)**, then T<sub>n</sub> forces T<sub>k</sub> to be killed − that is T<sub>n</sub> "wounds" T<sub>k</sub>. T<sub>k</sub> is restarted later with a random delay but with the same timestamp(k).

- **Timestamp(T<sub>n</sub>) > Timestamp(T<sub>k</sub>)**, then T<sub>n</sub> is forced to "wait" until the resource is available.

## Cost

**Abort cost peer transaction**

- New transaction generally hold fewer locks and have less data already read or written

- Old transaction generally hold more locks and hva more data already read or written

So wait-die costs less per abort transaction.

**Abort transaction quantity**

- Old transaction generally hold more locks

- New transaction generally have more lock requests

- Most conflicts are when new transaction try to acquire locks held by old transaction

So Wait-die will cause more abort transactions.

## Reference
- [What is the difference between wait-die and wound-wait algorithms? - Stackoverflow](https://stackoverflow.com/questions/32794142/what-is-the-difference-between-wait-die-and-wound-wait-deadlock-prevention-a)
- [有哪些分布式数据库可以提供交互式事务？ - 知乎](https://www.zhihu.com/question/344517681)
