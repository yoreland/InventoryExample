## SetBucketInventory
> 设置对应bucket清单的示例代码，每个bucket需要执行一次

## StartQueryExample
> 清单生成数据之后，使用这个示例代码进行查询。
> 查询之前需要先创建table，代码提供了``createTable()``方法供参考。
> 创建表成功后即可进行查询，示例提供了一个简单的按照stand和gda分类存储进行计费的sql，也可以自行使用sql查询。
> 每个prefix查询一次后结果类似如下,cost单位是元

```console
GB,storage_class,cost,price,
1527,STANDARD,276.387,0.181,
1527,GLACIER_DEEP_ARCHIVE,276.387,0.181,
1527,GLACIER_INSTANT_RETRIEVAL,276.387,0.181,
...
```