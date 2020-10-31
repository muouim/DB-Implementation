# 实验一聚集索引存储

### Git使用方法

```
git config

git init
git remote add origin https://github.com/OliveKong/poster.git

git pull origin master

git add .
git commit -m "shiyan"
git push -u origin master
```



### 单元测试方法

首先open打开（**打开前需删除build目录下test.dat和meta.db**）

```
.\bin\utest.exe db/table.h --section open
```

Insert插入

```
.\bin\utest.exe db/table.h --section insert 
```

迭代器测试insert结果

```
.\bin\utest.exe db/table.h --section iterator
```

find查找，根据迭代器判断是否正确

```
.\bin\utest.exe db/table.h --section find
```

update更新，根据迭代器判断是否正确

```
.\bin\utest.exe db/table.h --section update
```

remove更新

```
.\bin\utest.exe db/table.h --section remove
```

迭代器测试remove结果

```
.\bin\utest.exe db/table.h --section iterator
```

