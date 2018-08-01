# -*- mode: ruby -*-
# vi: set ft=ruby :

Vagrant.configure("2") do |config|
  config.vm.box = "centos/7"
  config.vm.provider "virtualbox" do |v|
    v.gui = true
  end
  config.ssh.forward_x11 = true
  config.vm.provision "shell", inline: <<-SHELL
    # Install access to Red-Hat packages for python and PostGIS
    sudo yum install -y epel-release
    # Install xauth for using the X11 forwarding
    sudo yum install xauth
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
    # Install python module matplotlib for creating gantts inside the excel files
    sudo yum install -y python34-tkinter
    # Install gcc as it is needed for installing python modules
    sudo yum install -y gcc
    sudo yum install -y python34-devel
    # Install pytest
    sudo yum install -y pytest.noarch
    sudo pip3 install pytest-cov
    # Allow local connections to the DDBB
    sudo cp /var/lib/pgsql/data/pg_hba.conf /var/lib/pgsql/data/pg_hba_bak.conf
    sudo sed -i 's/peer/trust/' /var/lib/pgsql/data/pg_hba.conf
    sudo sed -i 's/ident/trust/' /var/lib/pgsql/data/pg_hba.conf
    sudo service postgresql restart
    # Init database
    sudo /vagrant/datamodel/init_ddbb.sh -f /vagrant/datamodel/gsdm_data_model.sql
    # Install python terminal color features
    sudo pip3 install termcolor
    # Install python module coverage for making coverage analysis of code
    sudo pip3 install coverage
    # Install python module before_after for race condition tests
    sudo pip3 install before_after
    # Install gsdm
    cd /vagrant/
    sudo pip3 install -e src
  SHELL
end
