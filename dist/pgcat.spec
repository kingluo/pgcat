Name:       pgcat
Version:    0.1
Release:    1%{?dist}
Summary:    enhanced postgresql logical replication
License:    FIXME
Source0:    %{name}-%{version}.tar.gz

%define debug_package %{nil}

%description
enhanced postgresql logical replication

%prep
%setup -q

%build
cd cmd/pgcat
go build
cd -
cd cmd/pgcat_setup_lww
go generate
go build

%install
mkdir -p %{buildroot}/usr/bin
mkdir -p %{buildroot}/usr/share/pgcat
cp -a cmd/pgcat/pgcat %{buildroot}/usr/bin/
cp -a cmd/pgcat/pgcat.yml %{buildroot}/usr/share/pgcat/
cp -a cmd/pgcat_setup_lww/pgcat_setup_lww %{buildroot}/usr/bin/
cp -a cmd/pgcat_setup_lww/lww.yml %{buildroot}/usr/share/pgcat/

%files
/usr/bin/pgcat
/usr/bin/pgcat_setup_lww
/usr/share/pgcat/pgcat.yml
/usr/share/pgcat/lww.yml

%changelog
# let's skip this for now
