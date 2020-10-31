# DB-Implementation

### Git使用方法

```
//配置查看
git config

//初始化
git init
git remote add origin https://github.com/OliveKong/poster.git

//拉取
git pull origin master

//提交
git add .
git commit -m "simulator"
git push -u origin master
```

### Repo规范

```
/Prototype 系统原型

/InnoDB 聚集索引存储

/B+ Tree B+树索引优化

/Notes 实验报告/笔记整理
```

### 单元测试方法

```
.\bin\utest.exe db/table.h --section insert

.\bin\utest.exe db/table.h --section open
```

