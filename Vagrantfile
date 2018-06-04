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
    # Allow local connections to the DDBB
    sudo cp /var/lib/pgsql/data/pg_hba.conf /var/lib/pgsql/data/pg_hba_bak.conf
    sudo sed -i 's/peer/trust/' /var/lib/pgsql/data/pg_hba.conf
    sudo sed -i 's/ident/trust/' /var/lib/pgsql/data/pg_hba.conf
    sudo service postgresql restart
    # Create database
    sudo psql -U postgres -c "CREATE DATABASE gsdmdb;"
    # Apply extension to postGIS to the database
    sudo psql -U postgres -d gsdmdb -c "CREATE EXTENSION postgis;"
    # Create tables inside the database
    sudo psql -U postgres -d gsdmdb -f /vagrant/datamodel/gsdm_data_model.sql
    # Execute tests
    cd /vagrant/src/tests/
    python3 initial_test.py
    python3 bulk_ingest_events.py
    python3 bulk_ingest_events_multiprocessing.py
  SHELL
end
