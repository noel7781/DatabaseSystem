나의 B+ Tree에 Join을 구현하면서, 내가 구현한 Join 방법은
Index nested loop join방법이다.
이 방법을 선택한 이유는 너무나 자연스럽게 이미 B+ Tree가 Index를 build한 상태이고, 
key값으로 build했기 때문에 clustered상태라서 Index nested loop join방식을 선택했다.
메모리 선언은 출력할 결과물을 받기위한 buf[260]을 선언했는데, 8 + 128 + 8 + 128 + 3(,) + 1(NULL)을 포함해서 260byte의 buf를 선언했다.
그리고 pathname 입력받은 값으로 파일을 만드는데, 어차피 tree index 구조이므로 이미 sorting되있어서,
open 조건에 O_APPEND를 줘서 pwrite가 항상 맨뒷줄에 일어나도록 했다.
테이블 2개기 open되지 않았으면 에러메세지를 표시하며
구현방법으로 왼쪽 테이블의 key값을 먼저 찾는다.
그리고 오른쪽 테이블에서 해당 키 값을 갖는 값을 버퍼에 올리고 binary search를 통해 index를 찾는다.
찾았으면 buf에 담아서 write해주고 sync 시킨다.