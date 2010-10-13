rm -rf lib/genpyblocking rm -rf lib/genpy
thrift --gen py --gen py:twisted thrift/snowflake.thrift
mv gen-py lib/genpyblocking
mv gen-py.twisted lib/genpy