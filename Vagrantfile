# -*- mode: ruby -*-
# vi: set ft=ruby :

Vagrant.configure("2") do |config|
  
  config.vm.provider "virtualbox" do |v|
    v.gui = true
  end
  config.ssh.forward_x11 = true
  config.vm.define "centos" do |centos|
    centos.vm.box = "centos/7"
    centos.vm.provision "shell", inline: <<-SHELL
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
        # Install Sphinx for the automatic generation of the code documentation
        sudo pip3 install Sphinx
        # Install gsdm
        cd /vagrant/
        sudo pip3 install -e src
        # Inserting environment variables
        echo "# GSDM resources path" >> ~/.bashrc
        echo export GSDM_RESOURCES_PATH="/vagrant/src" >> ~/.bashrc
        source ~/.bashrc
      SHELL

  end

  config.vm.define "ubuntu" do |ubuntu|
    ubuntu.vm.box = "ubuntu/xenial64"
    ubuntu.vm.provision "shell", inline: <<-SHELL
        # Update the ubuntu
        sudo apt-get update
        # Install PostgreSQL
        sudo apt-get install -y postgresql
        # Install Postgis
        sudo apt-get install -y postgis
        # Allow local connections to the DDBB
        sudo sed -i 's/peer/trust/' /etc/postgresql/9.5/main/pg_hba.conf
        sudo sed -i 's/ident/trust/' /etc/postgresql/9.5/main/pg_hba.conf
        sudo sed -i 's/md5/trust/' /etc/postgresql/9.5/main/pg_hba.conf
        sudo service postgresql restart
        # Install python 3
        sudo apt-get install -y python3
        # Install python package manager
        sudo apt-get install -y python3-pip
        # Install python testing manager
        sudo apt-get install -y python-pytest
        sudo pip3 install pytest-cov
        # Install python terminal color features
        sudo pip3 install termcolor
        # Install python module coverage for making coverage analysis of code
        sudo pip3 install coverage
        # Install python module before_after for race condition tests
        sudo pip3 install before_after
        # Install Sphinx for the automatic generation of the code documentation
        sudo pip3 install Sphinx
        # Install texlive
        sudo apt-get install -y texlive texlive-latex-extra
        # Init database
        sudo /vagrant/datamodel/init_ddbb.sh -f /vagrant/datamodel/gsdm_data_model.sql
        # Install gsdm
        cd /vagrant/
        sudo pip3 install -e src
        # Inserting environment variables
        echo "# GSDM resources path" >> ~/.bashrc
        echo export GSDM_RESOURCES_PATH="/vagrant/src" >> ~/.bashrc
        source ~/.bashrc
      SHELL
  end

end
