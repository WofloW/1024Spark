spark-shell.py port
@Function: Do interactive tasks on driver
@argument port: port that driver will bind to.

spark-submit.py test_page_rank.py port
@Function: Do page rank on driver
@argument port: port that driver will bind to.
@argument path: path of the file to be parsed.
**This file must be number pairs. Please check pagerank.txt for input format.

spark-submit.py test_word_count_.py port
@Function: Do word count on driver
@argument port: port that driver will bind to.
@argument path: path of the file to be parsed.

spark-submit.py test_Ext.py port
@Function: Do topbykey on driver
@argument port: port that driver will bind to.
@argument path: path of the file to be sorted.
**This file must be a list of numbers. Please check sort.txt for input format.

ms_Worker.py driveripport port
@Function: Accept tasks from driver
@argument driveripport: ip:port that driver will bind to.
@argument port: port that worker will bind to.