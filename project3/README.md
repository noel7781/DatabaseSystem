이번에 추가된건 Buffer Manager로 기존의 디스크 단위로 입출력이 일어나던 걸 메모리에서 B+ Tree를 관리하기 위해서 버퍼를 추가한다.
여기서 buffer_control_block을 전역변수로 선언했기 때문에 buffer_write_page를 가지고 버퍼의 read와 write를 모두 할수 있게 만들고 따로 buffer_read_page를 만들지 않았다.
값을 추가하거나 제거하는 방법은 project 2에 buffer만 더한 형식인데, 페이지를 버퍼에 담아서 만약 페이지가 없으면 페이지를 가져오고, 페이지가 이미 버퍼에 있으면 좀더 빠르게 계산할 수 있도록 만들었다. pinpoint를 관리할 때 각 함수 내부에서 모두 pin 이 on off되게 만들었다. 물론 성능의 저하는 있겠지만 LRU방식으로 버퍼의 수가 어느정도만 되도 pin때문에 못읽는 일이 없기 때문에 correctness에서는 맞다고 생각한다.
사용한 page replacement policy는 LRU로 prev, next를 buffer_control_block의 주소를 줬으며 먼저 buffer_control_block의 0번을 헤더처럼 만들고, 만약 버퍼에 새로운 페이지를 추가하면, buffer_control_block의 prev에 달고, 헤더의 next로 가면서 pin이 꽂혀있지 않은 페이지를 evict 시킨다. 이때 만약 dirty bit가 켜져 있으면 이 버퍼페이지를 디스크에 내려서 기록한다. 만약 이미 버퍼상에 내가 부른 페이지가 이미 있다고 해도, 그 페이지가 담긴 버퍼를 가장 뒤로 보내서, 가장 사용이 안된 버퍼페이지를 가장 앞에 놓이게 만들었다.
모든 입출력 연산이 table_id가 관여하므로 fd배열을 myFile구조체로 만들어서 배열의 index를 table_id로 만들고 구조체안에서 is_used변수를 쓰는데 이 변수는 구조체안의 fd가 사용중이라는 의미 이며 만약 close_table함수를 부르면 is_used를  false로 만든다. fd자체를 없애는 것 보다 사용이 불가능한 상태로 보이게끔 만들었다.
만약 프로그램이 껐다 켜지면 myFile구조체에 fd값이 모두 0으로 초기화 될것이므로 다시 1번부터 넣어줄 수 있도록 구현하였다.
파일을 close시키면 fd배열의 값을 0으로 만들지 않고 그냥 is_used 비트만 표현했다.
file_write_page와 file_read_page는 project 2와 동일하지만 file_free_page와 file_alloc_page는 버퍼매니져에서 사용되므로 buffer_free_page와 buffer_alloc_page로 바꿨다.
shutdown_db()함수는 구현은 했지만 다른 함수내에서 이 함수를 call하는 함수는 없다.

지난번 과제와 마찬가지로 기본페이지의 사이즈는 8천으로 했고 만약 부족하다면 ctrl + f로 8000인걸 한번에 수정하면 된다.
별도의 empty tree를 나타낸다거나 하는 출력문들을 모두 주석처리했다.