# dataSync
!!!同步不涉及任何同步规则（过滤，替换）则XML只需完成最基本的表字段，表名对应关系即可。代码报错处可直接删除。
！！！打成jar包引入项目即可使用，test目录有完整测试类，依照xml在需要引入此工具的项目中新建相应xml即可（xml格式内容依照本项目的xml）
三个xml分别为同步xml，替换规则xml，过滤规则xml。
数据库同步工具
基于读取xml方式进行远程数据同步功能实现。
目前为同步远程sqlserver（理论上oracle也可以，未测试），采用多线程并发执行，批量操作，极大地提高了同步效率。
若同步mysql可将多线程分片读取数据时将分片规则进行相应修改，目前分片规则为rowId方式。
欢迎感兴趣者进行后续优化和扩展功能。
