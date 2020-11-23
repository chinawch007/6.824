# 6.824
MIT 6.824 Distributed Systems Project

### TestFigure8Unreliable2C   
>对paper中的图8特殊情况做测试.要适当增大leader向follower发消息的频率和发起选举的频率才能过.不能等到前一个消息的超时或失败再重试,不然样例测试会超时.
### GenericTestLinearizability   
>线性一致性的测试,测试原理可参见 https://www.zhihu.com/column/p/41632336, 代码实现基本按照这篇文章的描述
### TestChallenge1Delete   
>数据分片配置变更,某数据分片从某group转移走之后,要删掉该部分数据,样例会检查group中table大小
### TestChallenge2Unaffected   
>分片配置变更时,某group要转移走一部分数据,无需转移即仍由该group负责的数据分片仍能提供服务   
>测试代码中有bug,第3个for循环中把数据的key和管理key的shard id弄混了.不影响测试结果所以不容易发现,看课程2020年的代码还有这个bug.
### TestChallenge2Partial   
>分片配置变更时,某group只拉取到一部分分片数据,也应该能够处理这部分数据的请求
