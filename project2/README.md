Disk based B+Tree를 구현하면서, 나는 기본 페이지의 갯수를 8천개로 잡았다. (왠만한 양의 데이터를 받아들이기 쉽게 하기 위해서)
그리고 데이터 크기가 크면, 굉장히 긴 시간을 기다려야 연산이 끝난다.
만약 페이지의 갯수가 더 필요하다면 set_header  함수와 open 함수에서 for loop문 안의 숫자를 해당 페이지 숫자만큼 바꿔주면 된다.
그리고 페이지를 나타내는 구조체를 page_t에 union을 사용해서 모든 페이지 종류를 포현했다.
open_table 함수를 실행했을때 unique 한 table id를 갖기 위해 현재의 시간을 table_id로 사용했다.
나머지 insert, delete, find함수는 성공하면 0값을 리턴해주며 insert의 경우 duplicate key는 받지 않는다. delete는 있는 key값에 대해서만 연산을 진행하며 find는 만약 해당 key값이 존재한다면 value를 출력해주고, 아니면 오류메세지를 출력한다.
그리고 메인함수에 leaf페이지의 key값을 출력하는 print함수를 추가로 만들었다.
Disk based B+ Tree를 구현하려고 여러가지 함수를 정의 했는데,
Disk space manager 가  저장된 DB를 관리하기 위해 file_alloc_page, file_free_page, file_read_page, file_write_page를 만들었다.
file_alloc_page함수 여러 free page들 중 header page가 가리키는 free page의 번호를 리턴해주고, header페이지가 다음 free page를 가리키게 만드는 함수이다.
file_free_page는 페이지를 비워주고, 그 페이지를 다시 쓸수 있도록 header page가 비워진 free page를 가리키게 만드는 함수이다.
file_read_page와 file_write_page는 각각 해당 페이지번호에 페이지의 값을 쓰고 읽는 함수로 만들었다.

그럼 주 연산인 Insert와 Delete의 기능을 보면,
Insert는 먼저 key 값이 들어오면 이 key 값이 duplicate key인지 아닌지 판단한다. 만약 duplicate key라면 입력을 받지 않고, 해당 함수를 종료시킨다. 근데 중복된 키가 아니라면 입력된 값으로 (key, value)쌍을 가지는 record를 하나 만든다. 여기서 만약 b+ tree안에 아무런 값도 들어있지 않다고 하면 새로운 b+ tree를 만들어서 key값을 넣어준다. 그리고 만약 이미 b+ tree가 어떤 값을 가지고 있다고 하면, 값이 들어갈 적당한 위치를 찾아서 들어간다. order값보다 작다면 새로운 페이지를 만들지 않고 값을 추가하다가, 만약 order보다 큰 값이 들어오면 새로운 페이지를 만드는 일련의 과정들을 wiki - 1번에서 적은것처럼 과정이 이뤄진다. 약간 다른점은 페이지의 연산은 page number을 적는 칸이 한칸 더 있어서 node로 생각할때 포인터와 동일하게 생각할 수 있게 했다.
Delete도 wiki - 1 에서 적은 delete 과정을 따라가지만, 이 B+ Tree는 Delayed merge를 지원한다.
페이지의 key값이 order/2 가 되도 merge를 진행하지 않고, 만약 그 페이지의 key값이 0이 될때 비로소 merge를 진행하는 기능인데, 기존의 B+ Tree와 다르게 redistribution 과정이 사라진다. coalescence 함수를 실행하면  입력을 받은 후 입력을 받은 페이지 기준 왼쪽페이지와 merge를 하는 함수인데, 만약 입력을 받은 함수가 leftmost 상태라면 leftmost기준 한칸 오른쪽에 있는 페이지와 merge를 진행한다. key값이 0일때만 merge하므로, 다른페이지에 있는 모든 값들을 그대로 받아오면 된다.

///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////


Wiki라는것을 작성해본 경험이 없고 예시로 본 경험도 없어서 정확히 어떻게 적는지 모르겠습니다. 과제 만드는데만 시간을 너무 많이 써서 piazza에 질문도 올리지 못했는데, 혹시 예시로 참고할만한 wiki 하나만 메일로 보내주시면 다음번에는 참고해서 열심히 적도록 하겠습니다.
noel7781@naver.com 입니다. 감사합니다.