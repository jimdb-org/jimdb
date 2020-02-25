.. JIMDB_readthedoc documentation master file, created by
   sphinx-quickstart on Fri Nov  8 16:59:02 2019.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

JIMDB Documentation
===============================================

JIMDB is a cloud-native key-value and SQL database with intelligent storage tiering.

Our motivation is to build a scalable and fast enough database system which makes the frontend caching cluster not needed, but itself does not use too many RAM nodes. 

.. toctree::
   :maxdepth: 2
   :caption: Overview

   features
   overview/architecture


.. toctree::
   :maxdepth: 2
   :caption: Quick Start Guide

   deploy/singleBeta
   deploy/sourceBased

.. toctree::
   :maxdepth: 2
   :caption: User Documentation
   
   user-manual/redis/redis
   user-manual/sql/sql


.. toctree::
   :maxdepth: 2
   :caption: Design Documentation

   design/README


.. toctree::
   :maxdepth: 2
   :caption: Perfomance

   perfomance/perfomance


.. toctree::
   :maxdepth: 2
   :caption: FAQ

   faq/faq


.. toctree::
   :maxdepth: 2
   :caption: 查看源码

   github地址 <https://github.com/jimdb-org/jimdb>


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
