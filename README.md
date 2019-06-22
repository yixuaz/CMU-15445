# CMU-15445 
Schedule site
https://15445.courses.cs.cmu.edu/fall2018/schedule.html

Study blog for cmu 15 445
https://www.jianshu.com/nb/36265841

cmu_15445_2017.rar is the project origin source file.
 
cmu_15445_2017_done.rar is the project solution(including 4 projects). 


## Target
### 1. Correctness 
  My Solution make sure cover as most test cases as possible. 
  * project 1 14 tests
  * project 2 ~30 tests
  * project 3 20 tests
  * project 4 9 tests
  And pass at least 1000 times for project 2&3&4
### 2. Simple and Understandable
  * Concise code is always my aim.

## Blog [chinese]

- [x] Lab 1: [Buffer Pool](https://15445.courses.cs.cmu.edu/fall2018/project1/)
 * [lab 1 study note](https://www.jianshu.com/p/ede089d3d8ad)

- [x] Lab 2: [B+ Tree](https://15445.courses.cs.cmu.edu/fall2018/project2/)
 * [lab 2a study note](https://www.jianshu.com/p/628a39d03b79)
 * [lab 2b study note](https://www.jianshu.com/p/386e36991c64)
 * [lab 2c study note](https://www.jianshu.com/p/b83272f7684b)
  
- [x] Lab 3: [Concurrency Control](https://15445.courses.cs.cmu.edu/fall2017/project3/) Lack info when using 2018 project documentation, so change to 2017
 * [lab 3 study note](https://www.jianshu.com/p/087d23a17ce4)
- [x] Lab 4: [Logging & Recovery](https://15445.courses.cs.cmu.edu/fall2017/project4/)  Lack info during using 2018 project documentation, so change to 2017
 * [lab 4 study note](https://www.jianshu.com/p/88796027112b)
  
  ## For self-study, take care using it if u are a student
>You must write all the code you hand in for 6.824, except for code that we give you as part of the assignment. You are not allowed to look at anyone else's solution, you are not allowed to look at code from previous years, and you are not allowed to look at other Raft implementations. 

## Origin Readme
### Build
```
mkdir build
cd build
cmake ..
make
```
Debug mode:

```
cmake -DCMAKE_BUILD_TYPE=Debug ..
make
```

### Testing
```
cd build
make check
```

### Run virtual table extension in SQLite
Start SQLite with:
```
cd build
./bin/sqlite3
```

In SQLite, load virtual table extension with:

```
.load ./lib/libvtable.dylib
```
or load `libvtable.so` (Linux), `libvtable.dll` (Windows)

Create virtual table:  
1.The first input parameter defines the virtual table schema. Please follow the format of (column_name [space] column_type) seperated by comma. We only support basic data types including INTEGER, BIGINT, SMALLINT, BOOLEAN, DECIMAL and VARCHAR.  
2.The second parameter define the index schema. Please follow the format of (index_name [space] indexed_column_names) seperated by comma.
```
sqlite> CREATE VIRTUAL TABLE foo USING vtable('a int, b varchar(13)','foo_pk a')
```

After creating virtual table:  
Type in any sql statements as you want.
```
sqlite> INSERT INTO foo values(1,'hello');
sqlite> SELECT * FROM foo ORDER BY a;
a           b         
----------  ----------
1           hello   
```
See [Run-Time Loadable Extensions](https://sqlite.org/loadext.html) and [CREATE VIRTUAL TABLE](https://sqlite.org/lang_createvtab.html) for further information.

### Virtual table API
https://sqlite.org/vtab.html

### TODO
* update: when size exceed that page, table heap returns false and delete/insert tuple (rid will change and need to delete/insert from index)
* delete empty page from table heap when delete tuple
* implement delete table, with empty page bitmap in disk manager (how to persistent?)
* index: unique/dup key, variable key
