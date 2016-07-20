# <<敏捷数据>>

书可以在这买 [here](http://shop.oreilly.com/product/0636920025054.do)。 现在的问题，第二版都出来了，第一版虽然有中文版但是好多demo跑不通额。 


Don't forget to initialize your python environment. Try linux (apt-get, yum) or OS X (brew, port) packages if any of the requirements don't install in your virtualenv.

敏捷数据代码实例


Setup your Python Virtual Environment
-------------------------------------

这里通过vitrualenv可以配置好需要的python版本?通过requirement文件？


```
# From project root

# Setup python virtualenv
virtualenv -p `which python2.7` venv --distribute
source venv/bin/activate
pip install -r requirements.txt
```

Download your Gmail Inbox!
--------------------------

```
# From ch3

# Download your gmail inbox
cd gmail
./gmail.py -m automatic -u me@gmail.com -p 'my_password_' -s ./email.avro.schema -f '[Gmail]/All Mail' -o /tmp/test_mbox 2>&1 &
```

Chapter 2: Data
===============

An example spreadsheet is available at [ch02/Email Analysis.xlsb](https://github.com/rjurney/Agile_Data_Code/blob/master/ch02/Email%20Analysis.xlsb). Example Pig code is available at [ch02/probability.pig](https://github.com/rjurney/Agile_Data_Code/blob/master/ch02/probability.pig).

Chapter 3: Agile Tools
======================

Full tutorial in [Chapter 3 README](https://github.com/rjurney/Agile_Data_Code/tree/master/ch03).

Highlight:

Download your Gmail Inbox!
--------------------------

```
# From ch3

# Download your gmail inbox
cd gmail
./gmail.py -m automatic -u me@gmail.com -p 'my_password_' -s ./email.avro.schema -f '[Gmail]/All Mail' -o /tmp/test_mbox 2>&1 &
```

Chapter 4: To the Cloud!

[Chapter 4 tutorial](https://github.com/rjurney/Agile_Data_Code/tree/master/ch04)

Chapter 7: Collecting and Displaying Atomic Records
===================================================

[Chapter 7 tutorial](https://github.com/rjurney/Agile_Data_Code/tree/master/ch07)

Chapter 8: Creating Charts
==========================

[Chapter 8 tutorial](https://github.com/rjurney/Agile_Data_Code/tree/master/ch08)

Chapter 9: Building Interactive Reports
=======================================

[Chapter 9 tutorial](https://github.com/rjurney/Agile_Data_Code/tree/master/ch09)

Chapter 10: Making Predictions
==============================

[Chapter 10 tutorial](https://github.com/rjurney/Agile_Data_Code/tree/master/ch10)

Chapter 11: Driving Actions
===========================

[Chapter 11 tutorial](https://github.com/rjurney/Agile_Data_Code/tree/master/ch11)