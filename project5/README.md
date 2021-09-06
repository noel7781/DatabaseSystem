  Transaction의 ACID중 C와 I를 보장하는 Locking manager는, 여러 사용자가 사용한다는 가정을 바탕으로 쓰레드를 여러개 사용하여 여러 쓰레드가 동시에 접속해도 문제없도록 사용할 수 있게 만들어주는 수단이다. Lock manager을 구현하기 위해 필요한 것은 Transaction Table, Lock Table, Transaction, Lock object, abort 됬을때 undo할 수 있도록 해주는 Undolog가 필요하다.
 
구현함에 있어서 일단 모든 테이블 사이즈같은 변수들을 bpt.h파일에 상수로 정의하였다. 정의한 값으로는, 쓰레드 최대 40개, 락해쉬테이블 사이즈 10000, 트랜잭션 최대 40개로 정의했으며 bpt.h파일에서 수정 가능하다. 트랜잭션 id는 0부터 발급하나 트랜잭션테이블 배열의 빈자리가 생기면 들어가는걸로 정했다.

가지고 있는 Lock mode는 NOLOCK, SHARED, EXCLUSIVE로 lock hash table상에 인덱스를 안내해주는 lock object는 NOLOCK으로, 연산으로 불리는 lock object들은 각각 shared와 exclusive락으로 잡히고 transaction state는 IDLE, RUNNING, WAITING, END로 모든 과정이 끝난 트랜잭션들은 END 상태로 표기해줬다.

Begin Transaction시 새로운 트랜잭션을 만들어서 상태를 초기화해주고 트랜잭션 테이블에 추가해준다. End Transaction때는 잡고있던 lock을 release해주면서 나의 뒤에 자고있는 thread들을 깨워주는데, 만약 뒤에 오는 lock object가 exclusive면 그만 깨우고, lock shared상태면 일단 깨우고, 그 뒤까지 보는데 만약 shared가 있으면 연속적으로 깨우고 exclusive가 오면 깨우지 않는다. 그리고 트랜잭션 테이블에 있는 트랜잭션을 초기화시켜주고 쓰레드를 종료시킨다. Abort가 생기면 update중 생기는 undolog를 rollback해서 되살리고 end transaction을 호출하는 식으로 끝낸다.

이 프로그램을 구현할때 앞에있는 lock 의 mode가 강한 상태인지 약한상태인지 구별하지 않고 모든 lock object를 append시키는 방법을 사용했는데,
db_find는 lock mode가 shared로 잡히는데 먼저 버퍼풀의 래치을 잡고 타겟 버퍼 페이지를 가져오는데, 만약 이미 락이 잡혀있으면 버퍼풀래치를 풀고 다시 시도한다. 버퍼페이지의 래치를 얻는 단계가 성공한다면, 일단 table id와 page number로 hash값을 구하고 lock object를 추가할 준비를 하는데, 만약 처음 추가됬으면 state를 RUNNING상태로 바꾸고 lock 을 획득한다. 근데 처음이 아니라면, 앞에있는 lock을 살펴보게 되는데 앞에오는 lock이 lock을 획득한 상태가 아니거나 lock을 획득했는데, 나와 다른 트랜잭션이며 Exclusive상태일때는 해당 lock은 기본적으로 기다리게된다. 여기서 만약 해당 lock이 기다리는 lock이 이미 끝난 transaction의 lock이거나, 기다리고 있는 lock을 쫓아갔더니 나의 트랜잭션과 같은 record를 잡고있으며 나 자신의 트랜잭션이라면 Deadlock이나 conflict상황이 아니므로 해당 락을 획득하고 진행시킨다. 그런데 만약 다른 키를 가지고있다면 Deadlock 상황이며 abort시킨다. 자기 자신을 기다리는 상황이 발생하지 않는다면 conflict 상태로 트랜잭션은 앞선 lock이 깨워줄때까지 잠에 들게된다. 이 상황에 들어가지 않는다면 transaction의 상태를 RUNNING상태로 바꾸고 락을 획득한다. 이 모든걸 진행한 후 state가 RUNNING상태라면 해당 레코드를 찾아준다.

db_update는 lock mode가 exclusive로 잡히는데 db_find와 모든 과정이 유사하게 진행되지만 다른점은 앞에오는 lock과 비교를할때 find는 Exclusive상태일때 대기하지만 update는 exclusive와 shared상태일때 모두 기본적으로 기다리는 상태가된다. 쭉 동일하게 진행되다가 마지막부분에 transaction을 update할때 그냥 update만 하는게아니라 변경된 로그를 old value와 new value로 나눠서 로그를 저장해준다.

현재는 insert와 delete가 아닌 find와 update만 수행하므로 b+tree의 구조가 바뀌지 않는다. 그러므로 target page를 찾는 과정에서 별도의 latch를 생성하지 않았다. 추후에 insert와 delete를 지원할 때 target page에도 latch를 추가하겠다.

이 방법으로 find 와 update에 대한 lock manager을 구현하였다.