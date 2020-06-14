# 5 DBMS Transaction Projects Aim
- [x] 1. first project is to implement Recorvery Manager **ARIES** to ensure Durabilty and Atomicty. 
- [x] 2. second project is to implement Concurrency Control **MV-2PL** to ensure Isolation
- [ ] 3. third project is to implement Pessimistic distributed transactions based on first project (**2PC**)
- [ ] 4. fourth project is to implement LSM TREE KV STORE
- [ ] 5. fifth project is to implement Optimistic distributed transactions based on first project (**OCC**)

## DBMS_ARIES
a tiny database with ARIES recovery algorithm With WAL and Fuzzy Checkpoint to achieve ACID

## Introduction
This is a toy project which help you learn how RDBMS to achieve ACID and how ARIES work.

The Tiny DB only have one table with one column and ten rows data. each row is saved in one page.
it only support one function to increment one row.
after some action, client could do commit or abort for previous update.

in the test cases, it will increment all 10 rows data in one transaction and then commit or abort. 
so whenever db crash or do the steal flush policy or no-force policy. 
the tiny db could always recover from disaster and ACID could be kept. This is what ARIES did for us.

## Learning material
Before play with this project, if you don't know ARIES, I recommend you learn this lesson from CMU site.

https://15445.courses.cs.cmu.edu/fall2019/slides/21-recovery.pdf

there are also courese vedio on youtube.

https://www.youtube.com/watch?v=4VGkRXVM5fk&list=PLSE8ODhjZXjbohkNBWQs_otTrBTrjyohi&index=21


## Task description
In this project, you need to write 3 methods for ARIES 3 phases. you can search `TODO` easily find them.

You do not need to change any other java files except the 3 method to pass all test cases.

I prepare 7 test cases for you from simple situation to complex situation in `TinyDBClientTest.java`

all the test cases is random to make the tiny db crash.

## Getting started
so the first thing you need to pass is that there is a `Main` class which you could first try with your code. 

you could start by reading `TinyDBClient.java` and `Main.java`

this related to a demo from the bottom of the blog https://www.jianshu.com/p/ea61881309df to make sure your code could work on this demo.

I also leave my solution in this project if you do not have any idea after a long time thinking. 

Have a good journey.

