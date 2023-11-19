# 火山模型
将关系代数中的每个操作抽象成一个operator，然后将这些operator组合成一个operator tree，最后执行这个operator tree，从<u>根节点到叶子节点自上而下的调用Next方法</u>，这就是火山模型的基本思想。

例如SQL：
~~~SQL
SELECT Id, Name, Age, (Age - 30) * 50 AS Bonus
FROM People
WHERE Age > 30
~~~
![](https://i.imgur.com/HKwEXd5.png)

这里包含了3个**Operator**，首先用户调用最上面的**Operator**希望得到next tuple,在Project 调用子结点Select，而Select又调用子结点Scan，Scan从表中得到next tuple,Select会检查得到的tuple是否符合条件，如果不符合就请求Scan获取next tuple，如果满足，就返回给project，project对tuple中需要的字段进行筛选，当Scan发现没有数据可以返回的时候，就返回一个结束标记给上游已结束



# 火山模型的优缺点

火山模型

- 优点在于每个Operator都可以单独抽象实现，不需要关心其他Operator的逻辑

- 缺点在于每次都是只能计算一个tuple，这样就造成了多次调用Next，造成大量的函数调用的开销，导致CPU利用率不高

  > 在C++中，这些算子都是继承于一个公共的基类，如果每次都进行虚函数的调用的话，就会引发虚函数调用的开销，虚函数调用通常涉及额外的指针解引用和跳转，会消耗CPU的时间，每次计算都需要进行多次虚函数的调用，造成CPU使用率不高，

# 优化方向

**首先**:写一个循环去执行Operator，执行完之后再向上传递，将之前的自上而下的模型变成自下而上的模型

**其次**:火上模型改成一次Next方法读取多条tuple数据，这样可以将每次的Next方法开销进行分摊，这样CPU访问某个tuple中的某个列的时候，会将tuple加载到CPU Cache中（如果tuple小于Cache行大小），放后后续的列或少数的几个列，就能有较高的Cache命中率，

**最后**:编译时期实现，减少在运行的时候CPU的资源占用，