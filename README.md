# Huntaway (Warining: Stop maintenance. Only for tutorial.)
A twemproxy edition for redis cluster, revised on Twemproxy v2.4.0. Extensions include RW seperation, Hot/Cold data, and failover, etc.
����2013������һ��Twemproxy�İ棬Ϊ��ʵ��redis��Ⱥ����ʱ3.0��û����������ʵ����Redis�ļ�Ⱥ���ܣ�

Twemproxy �ֳ� nutcracker ����һ��memcache��redisЭ�������������һ������sharding ���м��������Twemproxy���ͻ��˲�ֱ�ӷ���Redis������������ͨ��twemproxy �����м����ӷ��ʡ� twemproxy Ϊ twitter ��Դ��Ʒ, Ŀǰ�汾���Բ鿴 [����]()

Redis ��һ����Դ��BSD��ɣ��ģ��ڴ��е����ݽṹ�洢ϵͳ���������������ݿ⡢�������Ϣ�м���� ��֧�ֶ������͵����ݽṹ���� �ַ�����strings���� ɢ�У�hashes���� �б�lists���� ���ϣ�sets���� ���򼯺ϣ�sorted sets�� �뷶Χ��ѯ�� bitmaps�� hyperloglogs �� ����ռ䣨geospatial�� �����뾶��ѯ�� Redis ������ ���ƣ�replication����LUA�ű���Lua scripting���� LRU�����¼���LRU eviction��������transactions�� �Ͳ�ͬ����� ���̳־û���persistence���� ��ͨ�� Redis�ڱ���Sentinel�����Զ� ������Cluster���ṩ�߿����ԣ�high availability����

2013-12-03�� NutCracker�޸ĳ���˼·��
�ڷ���������������ĸ����nc_server������group(>=0)��0��ʾû���飬Ϊ��������������ͬ��ŵķ�������Ϊһ����λ����ѡNC�Ĳ�����
����mΪ�������������з���������������һ�����а�������������nΪ1~m����
���еķ�����������ͬ�����ݡ�
д������ʹNC�ܹ����Ϸ������飬�������е���������д���ݣ�����α�֤���з���������ͬ������
��������NC��Ҫ���slave redis��������Ĭ��ֵ�£���Ϊ���������������ж�������
�޸������ļ���ʹ�����������Ϊһ�飻
崻�������������Ļ����ϣ�������ж���������е����n-1��������崻���NC�Ὣ��һ�����򻺴�ģ�������������Ϊ����������
���̣�
�޸Ĵ�ӡ֮�������ã�����nutcracker����ʹ����/usr/local/sbin�еľ��ļ���������/usr/local/bin����ģ�

>	Warning��NC��client����Ҫ����״̬����Redis Server���أ�ͨ��NC��client����Ҳ����NC�޷�ʵ��select��ԭ����ϸ�����롣
>	�������ļ��У������������ʶ���򣬱�����磺10.0.2.70:8604:1|1:1:m�������������ŷֱ��ǣ���š���Ȩ�غ����д��־��
������޷�������nutcracker����ֹͣ���޸ģ�OK��
>	������������conf_handler�����У����������Եõ���key:value�Խ���ɨ�裬Ȼ���볣��������conf_commands�еĹؼ��ֽ��ж��գ�������ִ�ж�Ӧ�����ú���������Է���������conf_add_server ()���������������IP��Port�Ȳ���������struct sockinfo{ int family; socklen_t addrlen; union { struct sockaddr_in in��}addr }�ṹ���У�
>	Nc_conf.c���ҵ����õĹؼ���
conf_add_server():delim[]���޸�delim[] = �� :::����Ȼ���ڶ�delim��ѭ���У���Ӷ����һ����:���Ĵ�������Ϊ��ѡ��
>	Nc_server.h����struct server���������ԣ�uint32_t gid��
>	Nc_server.c�������õ����������ĺ���conf_server_each_transform()�У���conf_server�н�group��ֵ��server->gid��
>	�ַ����̣�NC��ʼ��֮�����ڽ����¼�����proxy_recv�������ӵ���Ϣ�������proxy_recv�й�����������Ϣ���ա�����������conn_get�����У���  ����ķַ�����Ϣ���պ����У��и�server_pool_server��������һ��server pool��ѡ��һ��server���ַ������ĵ�·�ɻ��ƺܿ��ܻ��������޸ġ�
