# -*- mode: ruby -*-
# vi: set ft=ruby :

Vagrant.configure("2") do |config|
  config.vm.box = "centos/7"

  config.vm.provision "shell", inline: <<-SHELL
    # Install access to Red-Hat packages for python and PostGIS
    sudo yum install -y epel-release
    # Install PostgreSQL
    sudo yum install -y postgresql
    sudo yum install -y postgresql-server
    # Initialize PostgreSQL
    sudo postgresql-setup initdb
    # Enable and starting PostgreSQL service
    systemctl enable postgresql.service
    systemctl start postgresql.service
    # Install PostGIS
    sudo yum install -y postgis
    # Install python
    sudo yum install -y python34
    # Install python package manager
    sudo yum install -y python34-pip
    # Install python ORM for postgreSQL
    sudo pip3 install sqlalchemy
    # Install python DBApi for postgreSQL
    sudo pip3 install psycopg2
    # Install python ORM for postGIS
    sudo pip3 install geoalchemy2
    # Install python terminal color features
    sudo pip3 install termcolor
    # Install python terminal color features
    sudo pip3 install python-dateutil
    # Install python library for reading and writing Excel 2010
    sudo pip3 install openpyxl
    # Install python library for reading and writing Excel 2010
    sudo pip3 install lxml
    # Allow local connections to the DDBB
    sudo cp /var/lib/pgsql/data/pg_hba.conf /var/lib/pgsql/data/pg_hba_bak.conf
    sudo sed -i 's/peer/trust/' /var/lib/pgsql/data/pg_hba.conf
    sudo sed -i 's/ident/trust/' /var/lib/pgsql/data/pg_hba.conf
    sudo service postgresql restart
    # Init database
    sudo /vagrant/datamodel/init_ddbb.sh -f /vagrant/datamodel/gsdm_data_model.sql
    # Execute tests
    cd /vagrant/src/tests/
    python3 initial_test.py
    python3 bulk_ingest_events.py
    python3 bulk_ingest_events_multiprocessing.py
    python3 ingest_xml.py
    python3 extract_data_to_xls.py
  SHELL
end
